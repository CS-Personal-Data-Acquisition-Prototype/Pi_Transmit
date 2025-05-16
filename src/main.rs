use configparser::ini::Ini;
use rusqlite::{params, Connection};
use std::error::Error;
use std::io::{Error as IoError, ErrorKind, Write};
use std::net::TcpStream;
use std::thread;
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};
use std::path::Path;
use serde::Serialize;
use serde_json;

#[derive(Serialize)]
struct SensorData {
    session_id: Option<i32>,
    timestamp: String,
    latitude: f64,
    longitude: f64,
    altitude: f64,
    accel_x: f64,
    accel_y: f64,
    accel_z: f64,
    gyro_x: f64,
    gyro_y: f64,
    gyro_z: f64,
    dac_1: f64,
    dac_2: f64,
    dac_3: f64,
    dac_4: f64,
}

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
    
    // Database configuration
    let db_path = config.get("database", "path")
        .unwrap_or("../src/data_acquisition.db".to_string());
    
    // Server address for TCP connection
    let server_address = format!("{}:{}", server_ip, server_port);
    
    println!("Connecting to server at: {}", server_address);
    println!("Attempting to open database at: {}", &db_path);
    
    // Check if database file exists
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
    
    // Establish TCP connection
    let mut stream = match connect_with_retry(&server_address, max_retries, retry_delay) {
        Ok(stream) => stream,
        Err(e) => {
            println!("Failed to establish connection: {}", e);
            return Err(e);
        }
    };
    println!("TCP connection established successfully");
    
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
            
            // Create SensorData struct
            let sensor_data = SensorData {
                session_id: row.get(1)?,
                timestamp: row.get::<_, Option<String>>(2)?.unwrap_or_else(|| "None".to_string()),
                latitude: row.get(3)?,
                longitude: row.get(4)?,
                altitude: row.get(5)?,
                accel_x: row.get(6)?,
                accel_y: row.get(7)?,
                accel_z: row.get(8)?,
                gyro_x: row.get(9)?,
                gyro_y: row.get(10)?,
                gyro_z: row.get(11)?,
                dac_1: row.get(12)?,
                dac_2: row.get(13)?,
                dac_3: row.get(14)?,
                dac_4: row.get(15)?,
            };
    
            Ok(sensor_data)
        })?;
        
        // Send data to server
        for row_result in rows {
            match row_result {
                Ok(sensor_data) => {
                    // Try to send data via TCP
                    if let Err(e) = send_data(&mut stream, &sensor_data) {
                        println!("Error sending data: {}", e);
                        
                        // Try to reconnect if configured to do so
                        if auto_reconnect {
                            println!("Attempting to reconnect...");
                            match connect_with_retry(&server_address, max_retries, retry_delay) {
                                Ok(new_stream) => {
                                    stream = new_stream;
                                    // Retry sending this row
                                    if let Err(e) = send_data(&mut stream, &sensor_data) {
                                        println!("Failed to send data after reconnect: {}", e);
                                    } else {
                                        rows_count += 1;
                                    }
                                },
                                Err(e) => {
                                    println!("Failed to reconnect: {}", e);
                                    // Continue to next iteration, will try again after sleep
                                    break;
                                }
                            }
                        }
                    } else {
                        rows_count += 1;
                    }
                }
                Err(e) => println!("Error processing row: {}", e),
            }
        }

        // Send keep-alive if no data was sent
        if rows_count == 0 && keep_alive_interval > 0 {
            if let Err(e) = stream.write_all(b"KEEPALIVE\n") {
                println!("Failed to send keep-alive: {}", e);
                
                if auto_reconnect {
                    match connect_with_retry(&server_address, max_retries, retry_delay) {
                        Ok(new_stream) => stream = new_stream,
                        Err(e) => println!("Failed to reconnect: {}", e),
                    }
                }
            }
        }
            
        // Sleep for the specified interval before checking again
        thread::sleep(Duration::from_secs_f64(transmit_interval));
    }
        
    Ok(())
}

fn connect_with_retry(address: &str, max_retries: u32, retry_delay: u64) -> Result<TcpStream, Box<dyn Error>> {
    let mut attempts = 0;
    
    loop {
        match TcpStream::connect(address) {
            Ok(stream) => {
                println!("Connected to server at {}", address);
                return Ok(stream);
            },
            Err(e) => {
                attempts += 1;
                if attempts >= max_retries {
                    return Err(Box::new(e));
                }
                println!("Connection attempt {} failed: {}. Retrying in {} seconds...", 
                         attempts, e, retry_delay);
                thread::sleep(Duration::from_secs(retry_delay));
            }
        }
    }
}

fn send_data(stream: &mut TcpStream, data: &SensorData) -> Result<(), Box<dyn Error>> {
    // Serialize the data to JSON
    let json = serde_json::to_string(data)?;
    
    // Send the data followed by a newline
    stream.write_all(json.as_bytes())?;
    stream.write_all(b"\n")?;
    stream.flush()?;
    
    Ok(())
}

fn get_last_processed_id(conn: &Connection) -> Result<i64, Box<dyn Error>> {
    // Return the maximum rowid from the database
    let result: i64 = conn.query_row(
        "SELECT IFNULL(MAX(rowid), 0) FROM sensor_data",
        params![],
        |row| row.get(0),
    )?;
    
    Ok(result)
}