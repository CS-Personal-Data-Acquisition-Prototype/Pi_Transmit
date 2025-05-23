use configparser::ini::Ini;
use rusqlite::{params, Connection};
use std::error::Error;
use std::io::{Error as IoError, ErrorKind, Write, Read};
use std::net::TcpStream;
use std::thread;
use std::time::Duration;
use serde::Serialize;
use serde_json;
use std::net::Shutdown;

#[derive(Serialize, Clone)]
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
#[allow(dead_code)]
struct BatchedSensorData {
    datapoints: Vec<DataPoint>,
}

#[derive(Serialize)]
#[allow(dead_code)]
struct DataPoint {
    id: String,
    datetime: String,
    data_blob: String,
}

fn main() -> Result<(), Box<dyn Error>> {
    // Load configuration
    let mut config = Ini::new();
    config.load("config.ini")?;
    let config_send_mode = config.get("transmission", "send_mode").unwrap_or("batch".to_string());

    
    let args: Vec<String> = std::env::args().collect();

    // Determine sending mode with command line override capability
    let send_mode = if args.len() > 1 {
        args[1].as_str()
    } else {
        &config_send_mode
    };

    println!("Data transmission mode: {}", send_mode);

    // Server configuration
    let server_ip = config.get("server", "ip").unwrap_or("127.0.0.1".to_string());
    let server_port = config.get("server", "port").unwrap_or("9000".to_string());
    let max_retries = config.getuint("server", "max_retries")?.unwrap_or(3) as u32;
    let retry_delay = config.getuint("server", "retry_delay")?.unwrap_or(2) as u64;
    
    // Transmission configuration
    let continuous = config.getbool("transmission", "continuous")?.unwrap_or(true);
    let transmit_interval = config.getfloat("transmission", "transmit_interval")?.unwrap_or(1.0);
    let batch_size = config.getuint("transmission", "batch_size")?.unwrap_or(100) as u32;
    
    // Database configuration
    let db_path = config.get("database", "path")
    .unwrap_or("./data_acquisition.db".to_string());
    
    // Server address for TCP connection
    let server_address = format!("{}:{}", server_ip, server_port);
    
    println!("Connecting to server at: {}", server_address);
    println!("Attempting to open database at: {}", &db_path);
    
    // Connect to existing database
    println!("Opening existing database at: {}", &db_path);
    let conn = match Connection::open(&db_path) {
        Ok(conn) => {
            println!("Database opened successfully");
            conn
        },
        Err(e) => {
            println!("Failed to open database: {}", e);
            return Err(Box::new(e));
        }
    };
    
    // Track the last processed row ID
    let mut last_id = get_last_processed_id(&conn)?;

    // Main transmission loop
    while continuous {
        // Check if new data added
        let current_max_id: i64 = conn.query_row(
            "SELECT IFNULL(MAX(rowid), 0) FROM sensor_data",
            params![],
            |row| row.get(0)
        )?;

        println!("Current max ID in database: {} rows. Last processed ID: {}", current_max_id, last_id);

        if current_max_id > last_id {
            println!("Database has {} new rows since last check", current_max_id - last_id);

            let latest_timestamp: String = conn.query_row(
                "SELECT timestamp FROM sensor_data ORDER BY rowid DESC LIMIT 1",
                params![],
                |row| row.get(0)
            ).unwrap_or_else(|_| "No data".to_string());

            println!("Latest record timestamp: {}", latest_timestamp);
        }

        // Get row count to process
        let row_count: i64 = conn.query_row(
            &format!("SELECT COUNT(*) FROM sensor_data WHERE rowid > {}", last_id),
            params![],
            |row| row.get(0)
        )?;
        println!("Found {} rows to process", row_count);
        
        // Only connect if data to process
        if row_count > 0 {
            let mut rows_count = 0; 
            
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
            
            let rows = stmt.query_map(params![], |row| {
                let current_id = row.get::<_, i64>(0)?;
                
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
        
                Ok((current_id, sensor_data))
            })?;
            
            // Choose processing mode based on config
            if send_mode == "individual" {
                println!("Using individual processing mode");
                let mut _last_processed_id = last_id;
                // Process each item individually
                for row_result in rows {
                    match row_result {
                        Ok((row_id, sensor_data)) => {
                            match process_single_item(&server_address, &sensor_data, max_retries, retry_delay) {
                                Ok(_) => {
                                    // Success! Update the last_id and save it
                                    rows_count += 1;
                                    last_id = row_id; // Update when successful
                                    if let Err(e) = std::fs::write("last_processed_id.txt", last_id.to_string()) {
                                        println!("Warning: Failed to save last processed ID: {}", e);
                                    }
                                    _last_processed_id = row_id;
                                },
                                Err(e) => {
                                    // Connection failed, don't update last_id
                                    println!("ERROR: Single item processing failed: {}", e);
                                    break; // Break to avoid further errors
                                }
                            }
                        },
                        Err(e) => println!("Error processing row: {}", e),
                    }
                }
            } else {
                println!("Using batch processing mode");
                let mut current_batch: Vec<SensorData> = Vec::with_capacity(batch_size as usize);
                let mut last_processed_id = last_id;
                
                for row_result in rows {
                    match row_result {
                        Ok((row_id, sensor_data)) => {
                            // Add to the current batch
                            current_batch.push(sensor_data);
                            last_processed_id = row_id;
                            
                            // When batch is full, send it
                            if current_batch.len() >= batch_size as usize {
                                // Try to process the batch, only update last_id if successful
                                match process_batch(&server_address, &current_batch, max_retries, retry_delay) {
                                    Ok(_) => {
                                        // Success! Update the last_id and save it
                                        rows_count += current_batch.len();
                                        last_id = row_id; // Only update when successful
                                        if let Err(e) = std::fs::write("last_processed_id.txt", last_id.to_string()) {
                                            println!("Warning: Failed to save last processed ID: {}", e);
                                        }
                                        
                                        // Clear the batch after successful processing
                                        current_batch.clear();
                                    },
                                    Err(e) => {
                                        // Connection failed, don't update last_id
                                        println!("ERROR: Batch processing failed, will retry data: {}", e);
                                        
                                        // Implement backoff delay before retry
                                        println!("Waiting before retry...");
                                        thread::sleep(Duration::from_secs(retry_delay));
                                        
                                        // Break out of the loop to retry from the beginning
                                        break;
                                    }
                                }
                            }
                        },
                        Err(e) => println!("Error processing row: {}", e),
                    }
                }
                
                // Process any remaining rows in a final batch
                if !current_batch.is_empty() {
                    match process_batch(&server_address, &current_batch, max_retries, retry_delay) {
                        Ok(_) => {
                            // Success! Update the last_id
                            rows_count += current_batch.len();
                            last_id = last_processed_id;  // Update with highest processed ID
                            
                            println!("Successfully processed batch. New last_id: {}", last_id);
                            
                            // Save to file immediately after successful processing
                            if let Err(e) = std::fs::write("last_processed_id.txt", last_id.to_string()) {
                                println!("Warning: Failed to save last processed ID: {}", e);
                            }
                        },
                        Err(e) => {
                            // Connection failed, don't update last_id
                            println!("ERROR: Final batch processing failed, will retry data: {}", e);
                        }
                    }
                }
            }
            
            println!("Loop complete. Processed {} rows. New last_id: {}", rows_count, last_id);
        } else {
            // No data to process
            println!("No new data to process");
        }

        // Sleep for the specified interval before checking again
        thread::sleep(Duration::from_secs_f64(transmit_interval));
    }
    
    Ok(())
}


#[allow(dead_code)]
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


fn process_batch(server_address: &str, batch: &Vec<SensorData>, max_retries: u32, retry_delay: u64) -> Result<(), Box<dyn Error>> {
    // Build datapoints to match expected format
    let mut datapoints_json_array = Vec::with_capacity(batch.len());
    
    for sensor_data in batch {
        // Get the session ID as a string
        let id_str = match sensor_data.session_id {
            Some(id) => id.to_string(),
            None => "1".to_string()
        };
        
        // Create a datapoint object with data_blob as a direct JSON object
        let datapoint = serde_json::json!({
            "id": 1i64,
            "datetime": sensor_data.timestamp,
            "data_blob": {  // Direct JSON object, not a string
                "accel_x": sensor_data.accel_x,
                "accel_y": sensor_data.accel_y,
                "accel_z": sensor_data.accel_z,
                "lat": sensor_data.latitude,
                "lon": sensor_data.longitude, 
                "alt": sensor_data.altitude,
                "gyro_x": sensor_data.gyro_x,
                "gyro_y": sensor_data.gyro_y,
                "gyro_z": sensor_data.gyro_z,
                "dac_1": sensor_data.dac_1,
                "dac_2": sensor_data.dac_2,
                "dac_3": sensor_data.dac_3,
                "dac_4": sensor_data.dac_4, 
                "string": sensor_data.session_id.map_or(1i64, |id| id as i64)  
            }
        });
        
        datapoints_json_array.push(datapoint);
    }
    
    // Create the final JSON structure
    let batch_payload = serde_json::json!({
        "datapoints": datapoints_json_array
    });
    
    // Serialize to final JSON string
    let final_json = serde_json::to_string(&batch_payload)?;
    println!("Final JSON beginning: {}", &final_json[0..std::cmp::min(final_json.len(),100)]);
    

    // When sending the JSON:
    let mut attempts = 0;

    // Use this JSON directly instead of serializing batched_data
    let json = final_json;
   
    while attempts < max_retries {
        match TcpStream::connect(server_address) {
            Ok(mut stream) => {
                // Connection succeeded, now try to send data
                println!("Connected to server at {} for batch processing (attempt {}/{})", 
                         server_address, attempts + 1, max_retries);
                
                // Convert to JSON
                // let json = serde_json::to_string(&batched_data)?;
                let endpoint = "/sessions-sensors-data/batch";
                
                // Try to send data
                println!("Sending batch of {} records ({} bytes)", batch.len(), json.len());
                
                // Catch and handle any errors during send
                let send_result = (|| -> Result<(), Box<dyn Error>> {
                    stream.write_all(format!(
                        "POST {} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}", 
                        endpoint, json.len(), json
                    ).as_bytes())?;
                    
                    stream.flush()?;
                    stream.shutdown(Shutdown::Write)?;
                    
                    // Read response with timeout to confirm receipt
                    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
                    let mut buffer = [0; 1024];
                    
                    match stream.read(&mut buffer) {
                        Ok(n) if n > 0 => {
                            let response = String::from_utf8_lossy(&buffer[0..n]);
                            println!("Response: {} bytes - {}", n, response);
                            
                            // Check if the response contains a 400 Bad Request error
                            if response.contains("400 Bad Request") {
                                println!("ERROR: Server rejected the request format: {}", 
                                        response.lines().last().unwrap_or("Unknown error"));
                                
                                // Check if it's treating the batch as processed despite the error
                                if response.contains("Invalid request body") {
                                    println!("ERROR: The server rejected our data format.");
                                    return Err(Box::new(IoError::new(
                                        ErrorKind::InvalidData,
                                        format!("Bad request: {}", response.lines().last().unwrap_or("Unknown error"))
                                    )));
                                }
                                
                                return Err(Box::new(IoError::new(
                                    ErrorKind::InvalidData,
                                    format!("Server returned 400 Bad Request. Check data format")
                                )));
}
                            
                            // Check if the response contains a 404 Not Found error
                            if response.contains("404 Not Found") {
                                println!("ERROR: Endpoint not found on server. Using wrong URL?");
                                return Err(Box::new(IoError::new(
                                    ErrorKind::NotFound,
                                    format!("Server returned 404 Not Found. Endpoint '{}' doesn't exist", endpoint)
                                )));
                            }
                            
                            // Success!
                            return Ok(());
                        },
                        Ok(_) => {
                            println!("Server closed connection without sending data");

                            return Ok(());
                        },
                        Err(e) => {
                            if e.kind() == ErrorKind::WouldBlock || e.kind() == ErrorKind::TimedOut {
                                println!("No response within timeout period");

                                return Ok(());
                            } else {
                                println!("Error reading response: {}", e);
                                return Err(Box::new(e));
                            }
                        }
                    }
                })();
                
                // Close the connection completely regardless of success/failure
                let _ = stream.shutdown(Shutdown::Both);
                
                // If send was successful, return success
                if send_result.is_ok() {
                    println!("Batch successfully processed");
                    return Ok(());
                }
                
                // Otherwise, increment attempts and try again
                println!("Send failed: {:?}", send_result);
                attempts += 1;
            },
            Err(e) => {
                // Connection failed
                attempts += 1;
                println!("Connection failed (attempt {}/{}): {}", 
                         attempts, max_retries, e);
                
                if attempts >= max_retries {
                    return Err(Box::new(e));
                }
                
                // Wait before retry
                println!("Retrying in {} seconds...", retry_delay);
                thread::sleep(Duration::from_secs(retry_delay));
            }
        }
    }
    
    Err(Box::new(IoError::new(
        ErrorKind::ConnectionAborted,
        format!("Failed to send batch after {} attempts", max_retries)
    )))
}


/// Process a single sensor data item
fn process_single_item(server_address: &str, sensor_data: &SensorData, max_retries: u32, retry_delay: u64) -> Result<(), Box<dyn Error>> {
    let mut attempts = 0;
    
    // Create the JSON for a single item - using SAME format as batch
    let datapoint = serde_json::json!({
        "id": 1i64,
        "datetime": sensor_data.timestamp,
        "data_blob": {  // Direct JSON object, not a string
            "accel_x": sensor_data.accel_x,
            "accel_y": sensor_data.accel_y,
            "accel_z": sensor_data.accel_z,
            "lat": sensor_data.latitude,
            "lon": sensor_data.longitude, 
            "alt": sensor_data.altitude,
            "gyro_x": sensor_data.gyro_x,
            "gyro_y": sensor_data.gyro_y,
            "gyro_z": sensor_data.gyro_z,
            "dac_1": sensor_data.dac_1,
            "dac_2": sensor_data.dac_2,
            "dac_3": sensor_data.dac_3,
            "dac_4": sensor_data.dac_4, 
            "string": sensor_data.session_id.map_or(1i64, |id| id as i64)  
        }
    });
    
    // Debug the structure
    println!("Individual data JSON structure: {}", serde_json::to_string_pretty(&datapoint)?);
    
    // Single endpoint
    let single_endpoint = "/sessions-sensors-data";
    let json = serde_json::to_string(&datapoint)?;
    
    // Perform connection and send logic
    while attempts < max_retries {
        match TcpStream::connect(server_address) {
            Ok(mut stream) => {
                println!("Connected to server at {} for single item processing", server_address);
                
                // Send with proper HTTP headers including Connection: close
                let http_request = format!(
                    "POST {} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}", 
                    single_endpoint, json.len(), json
                );
                
                println!("Sending single record ({} bytes)", json.len());
                stream.write_all(http_request.as_bytes())?;
                stream.flush()?;
                stream.shutdown(Shutdown::Write)?;
                
                // Read response with timeout
                stream.set_read_timeout(Some(Duration::from_secs(5)))?;
                let mut buffer = [0; 1024];
                
                match stream.read(&mut buffer) {
                    Ok(n) if n > 0 => {
                        let response = String::from_utf8_lossy(&buffer[0..n]);
                        println!("Response: {} bytes - {}", n, response);
                        
                        // Add error handling similar to batch process
                        if response.contains("400 Bad Request") || response.contains("404 Not Found") {
                            println!("ERROR: Server rejected single item: {}", 
                                    response.lines().last().unwrap_or("Unknown error"));
                            attempts += 1;
                            continue;
                        }
                        
                        // Success!
                        return Ok(());
                    },
                    Ok(_) => {
                        // No data, but connection was closed normally
                        return Ok(());
                    },
                    Err(e) => {
                        if e.kind() == ErrorKind::WouldBlock || e.kind() == ErrorKind::TimedOut {
                            // Timeout, but data was sent
                            return Ok(());
                        } else {
                            println!("Error reading response: {}", e);
                            attempts += 1;
                            continue;
                        }
                    }
                }
            },
            Err(e) => {
                attempts += 1;
                println!("Connection failed (attempt {}/{}): {}", 
                         attempts, max_retries, e);
                
                if attempts >= max_retries {
                    return Err(Box::new(e));
                }
                
                thread::sleep(Duration::from_secs(retry_delay));
            }
        }
    }
    
    Err(Box::new(IoError::new(
        ErrorKind::ConnectionAborted,
        format!("Failed to send single item after {} attempts", max_retries)
    )))
}


fn get_last_processed_id(conn: &Connection) -> Result<i64, Box<dyn Error>> {
    // Check if stored last ID from previous runs
    let stored_id = std::fs::read_to_string("last_processed_id.txt").ok();
    
    if let Some(id_str) = stored_id {
        if let Ok(id) = id_str.trim().parse::<i64>() {
            return Ok(id);
        }
    }
    
    // If no stored ID, get the SECOND row's ID (or 0 if less than 2 rows exist)
    let second_row_id: i64 = conn.query_row(
        "SELECT IFNULL((SELECT rowid FROM sensor_data ORDER BY rowid LIMIT 1 OFFSET 1), 0)",
        params![],
        |row| row.get(0),
    )?;
    
    Ok(second_row_id)
}