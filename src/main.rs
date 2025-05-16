use configparser::ini::Ini;
use rusqlite::{params, Connection};
use std::error::Error;
use std::io::{Error as IoError, ErrorKind};
use std::thread;
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};
use std::path::Path;
use serde::Serialize;
use reqwest::blocking::{Client, Response};
use reqwest::header::{HeaderMap, HeaderValue, CONTENT_TYPE};

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

#[derive(Serialize)]
struct KeepAliveMessage {
    message_type: String,
    client_id: String,
    timestamp: i64
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
    
    // HTTP endpoint configuration
    let endpoint = format!("http://{}:{}/data", server_ip, server_port);
    let keep_alive_endpoint = format!("http://{}:{}/keepalive", server_ip, server_port);
    
    println!("Using API endpoint: {}", endpoint);
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
    
    // Create HTTP client
    let client = create_http_client(connection_timeout)?;
    println!("HTTP client created successfully");
    
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
                    // Try to send data via HTTP
                    if let Err(e) = send_data_http(&client, &endpoint, &sensor_data, max_retries, retry_delay) {
                        println!("Error sending data: {}", e);
                    } else {
                        rows_count += 1;
                    }
                }
                Err(e) => println!("Error processing row: {}", e),
            }
        }

        // Send keep-alive if no data was sent
        if rows_count == 0 && keep_alive_interval > 0 {
            // Create a simple keepalive message
            let keep_alive = KeepAliveMessage {
                message_type: "keepalive".to_string(),
                client_id: "pi_transmit".to_string(),
                timestamp: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs() as i64
            };
            
            // Send keepalive via HTTP instead of TCP stream
            match client.post(&keep_alive_endpoint)
                .json(&keep_alive)
                .send() 
            {
                Ok(response) => {
                    if !response.status().is_success() {
                        println!("Keepalive response error: {}", response.status());
                    }
                },
                Err(e) => println!("Failed to send keep-alive: {}", e)
            }
        }  
            
        // Sleep for the specified interval before checking again
        thread::sleep(Duration::from_secs_f64(transmit_interval));
            } // <- Missing closing brace for while loop
        
   Ok(())
}
fn create_http_client(timeout_seconds: u64) -> Result<Client, Box<dyn Error>> {
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    
    let client = Client::builder()
        .timeout(Duration::from_secs(timeout_seconds))
        .default_headers(headers)
        .build()?;
        
    Ok(client)
}

fn send_data_http(client: &Client, endpoint: &str, data: &SensorData, max_retries: u32, retry_delay: u64) -> Result<(), Box<dyn Error>> {
    let mut attempts = 0;
    
    while attempts < max_retries {
        match client.post(endpoint)
            .json(data)
            .send()
        {
            Ok(response) => {
                if response.status().is_success() {
                    return Ok(());
                } else {
                    println!("Server returned error: {} - {}", response.status(), response.text().unwrap_or_default());
                }
            },
            Err(e) => {
                println!("HTTP request error (attempt {}): {}", attempts + 1, e);
            }
        }
        
        attempts += 1;
        if attempts < max_retries {
            thread::sleep(Duration::from_secs(retry_delay));
        }
    }
    
    Err(Box::new(IoError::new(
        ErrorKind::ConnectionRefused,
        format!("Failed to send data after {} attempts", max_retries)
    )))
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