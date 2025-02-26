use configparser::ini::Ini;
use rusqlite::{params, Connection};
use std::error::Error;
use std::io::{Error as IoError, ErrorKind, Write};
use std::net::TcpStream;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::path::Path;

fn main() -> Result<(), Box<dyn Error>> {
    // Load configuration
    let mut config = Ini::new();
    config.load("config.ini")?;
    
    // Server configuration
    let server_ip = config.get("server", "ip").unwrap_or("127.0.0.1".to_string());
    let server_port = config.get("server", "port").unwrap_or("9000".to_string());
    let max_retries = config.getuint("server", "max_retries")?.unwrap_or(3) as u32;
    let retry_delay = config.getuint("server", "retry_delay")?.unwrap_or(2) as u64;
    
    // Connection configuration
    let keep_alive_interval = config.getuint("connection", "keep_alive_interval")?.unwrap_or(30) as u64;
    let connection_timeout = config.getuint("connection", "connection_timeout")?.unwrap_or(60) as u64;
    let auto_reconnect = config.getbool("connection", "auto_reconnect")?.unwrap_or(true);
    
    // Transmission configuration
    let continuous = config.getbool("transmission", "continuous")?.unwrap_or(true);
    let transmit_interval = config.getfloat("transmission", "transmit_interval")?.unwrap_or(1.0);
    let batch_size = config.getuint("transmission", "batch_size")?.unwrap_or(100) as u32;
    
    println!("Connecting to {}:{}", server_ip, server_port);
    
    // Open the local SQLite database
    // Database configuration - add to config.ini
    let db_path = config.get("database", "path")
        .unwrap_or("../Pi_TCP/src/data_acquisition.db".to_string());
    
    println!("Attempting to open database at: {}", &db_path);
    
    // Check if file exists before attempting to open
    if !Path::new(&db_path).exists() {
        println!("Database file not found at: {}", &db_path);
        println!("Current working directory: {:?}", std::env::current_dir()?);
        return Err(Box::new(IoError::new(
            ErrorKind::NotFound,
            format!("Database file not found at: {}", db_path)
        )));
    }
    
    // Open the local SQLite database
    let conn = match Connection::open(&db_path) {
        Ok(conn) => conn,
        Err(e) => {
            println!("Failed to open database: {}", e);
            return Err(Box::new(e));
        }
    };
    
    // Track the last processed row ID
    let mut last_id = get_last_processed_id(&conn)?;
    
    // Connect to the remote server
    let mut stream = connect_with_retry(&format!("{}:{}", server_ip, server_port), max_retries, retry_delay)?;
    println!("Connected to remote server at {}:{}", server_ip, server_port);
    
    // Main transmission loop
    while continuous {
        // Get new data since last transmission
        let query = format!(
            "SELECT 
                rowid, sessionID, timestamp, latitude, longitude, altitude, 
                accel_x, accel_y, accel_z, 
                gyro_x, gyro_y, gyro_z, 
                dac_1, dac_2, dac_3, dac_4
             FROM sensor_data 
             WHERE rowid > {}
             ORDER BY rowid
             LIMIT {}",
            last_id, batch_size
        );
        
        let mut stmt = conn.prepare(&query)?;
        let mut rows_count = 0;
        
        let rows = stmt.query_map(params![], |row| {
            let current_id = row.get::<_, i64>(0)?;
            last_id = current_id; // Update the last processed ID
            
            // Format CSV line
            Ok(format!(
                "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
                row.get::<_, Option<i32>>(1)?.map_or("None".to_string(), |v| v.to_string()),
                row.get::<_, Option<String>>(2)?.unwrap_or_else(|| "None".to_string()),
                row.get::<_, f64>(3)?,
                row.get::<_, f64>(4)?,
                row.get::<_, f64>(5)?,
                row.get::<_, f64>(6)?,
                row.get::<_, f64>(7)?,
                row.get::<_, f64>(8)?,
                row.get::<_, f64>(9)?,
                row.get::<_, f64>(10)?,
                row.get::<_, f64>(11)?,
                row.get::<_, f64>(12)?,
                row.get::<_, f64>(13)?,
                row.get::<_, f64>(14)?,
                row.get::<_, f64>(15)?,
            ))
        })?;
        
        // Send data to server
        for row_result in rows {
            match row_result {
                Ok(csv_line) => {
                    // Try to send data, reconnect if necessary
                    if let Err(e) = send_data(&mut stream, &csv_line) {
                        println!("Error sending data: {}", e);
                        
                        if auto_reconnect {
                            println!("Attempting to reconnect...");
                            match connect_with_retry(&format!("{}:{}", server_ip, server_port), max_retries, retry_delay) {
                                Ok(new_stream) => stream = new_stream,
                                Err(e) => {
                                    println!("Failed to reconnect: {}", e);
                                    // Continue to next iteration, will try again after sleep
                                    break;
                                }
                            }
                            
                            // Retry sending this row
                            if let Err(e) = send_data(&mut stream, &csv_line) {
                                println!("Failed to send data after reconnect: {}", e);
                            }
                        }
                    }
                    rows_count += 1;
                }
                Err(e) => println!("Error processing row: {}", e),
            }
        }
        
        // Send keep-alive if no data was sent
        if rows_count == 0 && keep_alive_interval > 0 {
            if let Err(e) = stream.write_all(b"KEEPALIVE\n") {
                println!("Failed to send keep-alive: {}", e);
                
                if auto_reconnect {
                    match connect_with_retry(&format!("{}:{}", server_ip, server_port), max_retries, retry_delay) {
                        Ok(new_stream) => stream = new_stream,
                        Err(e) => println!("Failed to reconnect: {}", e),
                    }
                }
            }
        } else if rows_count > 0 {
            println!("Sent {} rows to server", rows_count);
        }
        
        // Sleep for the specified interval
        thread::sleep(Duration::from_secs_f64(transmit_interval));
    }
    
    Ok(())
}

fn connect_with_retry(address: &str, max_retries: u32, retry_delay: u64) -> Result<TcpStream, Box<dyn Error>> {
    let mut attempts = 0;
    
    loop {
        match TcpStream::connect(address) {
            Ok(stream) => {
                // Configure socket
                stream.set_nodelay(true)?;
                return Ok(stream);
            }
            Err(e) => {
                attempts += 1;
                if attempts >= max_retries {
                    return Err(Box::new(IoError::new(
                        ErrorKind::ConnectionRefused,
                        format!("Failed to connect after {} attempts: {}", max_retries, e),
                    )));
                }
                println!("Connection attempt {} failed: {}. Retrying in {} seconds...", 
                          attempts, e, retry_delay);
                thread::sleep(Duration::from_secs(retry_delay));
            }
        }
    }
}

fn send_data(stream: &mut TcpStream, data: &str) -> Result<(), Box<dyn Error>> {
    stream.write_all(data.as_bytes())?;
    stream.write_all(b"\n")?;
    Ok(())
}

fn get_last_processed_id(conn: &Connection) -> Result<i64, Box<dyn Error>> {
    // Return 0 if no rows exist yet, otherwise the maximum rowid
    let result: i64 = conn.query_row(
        "SELECT IFNULL(MAX(rowid), 0) FROM sensor_data",
        params![],
        |row| row.get(0),
    )?;
    
    // Start from 0 to include all rows in first transmission
    Ok(0)
}