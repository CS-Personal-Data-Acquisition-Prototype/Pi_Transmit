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

mod db_pool;
use db_pool::{ConnectionPool};

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
    let skip_interval = config.getuint("transmission", "skip_interval")?.unwrap_or(1) as i64;

    let args: Vec<String> = std::env::args().collect();

    // Determine sending mode with command line override capability
    let send_mode = if args.len() > 1 {
        args[1].as_str()
    } else {
        &config_send_mode
    };

    // Persistent error handling
    let max_row_errors_per_batch = 3; 
    let mut row_error_count = 0;
    let mut should_restart_query = false; 

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

    let pool_size = config.getuint("database", "pool_size")?.unwrap_or(3) as usize;
    let timeout = config.getuint("database", "timeout")?.unwrap_or(30) as u64 * 1000; // Convert to ms
    let busy_timeout = config.getuint("database", "busy_timeout")?.unwrap_or(5000) as u64;
    
    // Server address for TCP connection
    let server_address = format!("{}:{}", server_ip, server_port);
    
    println!("Connecting to server at: {}", server_address);
    
    // Connect to existing database
    let conn_pool = match ConnectionPool::new(&db_path, pool_size, timeout, busy_timeout) {
        Ok(pool) => {
            pool
        },
        Err(e) => {
            println!("Failed to create database pool: {}", e);
            return Err(Box::new(e));
        }
    };
    
    let conn = conn_pool.get()?;
    let mut last_id = get_last_processed_id(&conn)?;

        // Check if last_id is valid for current database
    let max_id_in_db: i64 = conn.query_row(
        "SELECT IFNULL(MAX(rowid), 0) FROM sensor_data",
        params![],
        |row| row.get(0)
    )?;
    
    if last_id > max_id_in_db {
        println!("WARNING: Last processed ID ({}) is greater than max ID in database ({})", 
                 last_id, max_id_in_db);
        println!("Resetting last processed ID to {}", max_id_in_db);
        
        last_id = max_id_in_db;
        
        // Save the corrected ID
        if let Err(e) = std::fs::write("last_processed_id.txt", last_id.to_string()) {
            println!("Warning: Failed to save corrected last processed ID: {}", e);
        }
    }

    // Track the last processed row ID
    let mut last_id = get_last_processed_id(&conn)?;

    // Main transmission loop
    while continuous {

        let conn = match conn_pool.get() {
            Ok(conn) => conn,
            Err(e) => {
                println!("Error getting connection from pool: {}", e);
                thread::sleep(Duration::from_secs_f64(transmit_interval));
                continue;
            }
        };

        let current_max_id: i64 = conn.query_row(
            "SELECT IFNULL(MAX(rowid), 0) FROM sensor_data",
            params![],
            |row| row.get(0)
        )?;

        println!("Current max ID in database: {} rows. Last processed ID: {}", current_max_id, last_id);

        if current_max_id > last_id {
            let latest_timestamp: String = conn.query_row(
                "SELECT timestamp FROM sensor_data ORDER BY rowid DESC LIMIT 1",
                params![],
                |row| row.get(0)
            ).unwrap_or_else(|_| "No data".to_string());

            println!("Latest record timestamp: {}", latest_timestamp);
        }

        // Get row count to process
        let row_count: i64 = conn.query_row(
            &format!("SELECT COUNT(*) FROM sensor_data WHERE rowid > {} AND rowid % {} = 0", last_id, skip_interval),
            params![],
            |row| row.get(0)
        )?;
        
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
                 WHERE rowid > {} AND rowid % {} = 0
                 ORDER BY rowid
                 LIMIT {}",
                last_id, skip_interval, batch_size
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

                for row_result in rows {
                    match row_result {
                        Ok((row_id, sensor_data)) => {
                            match process_single_item(&server_address, &sensor_data, max_retries, retry_delay) {
                                Ok(_) => {
                                    // Success! Update the last_id and save it
                                    rows_count += 1;
                                    last_id = row_id;
                                    if let Err(e) = std::fs::write("last_processed_id.txt", last_id.to_string()) {
                                        println!("Warning: Failed to save last processed ID: {}", e);
                                    }
                                    _last_processed_id = row_id;
                                },
                                Err(e) => {
                                    // Connection failed, don't update last_id
                                    println!("ERROR: Single item processing failed: {}", e);
                                    break;
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
                                match process_batch(&server_address, &current_batch, max_retries, retry_delay, last_id) {
                                    Ok(_) => {
                                        // Success! Update the last_id and save it
                                        rows_count += current_batch.len();
                                        last_id = row_id;
                                        if let Err(e) = std::fs::write("last_processed_id.txt", last_id.to_string()) {
                                            println!("Warning: Failed to save last processed ID: {}", e);
                                        }
                                        
                                        // Clear the batch after successful processing
                                        current_batch.clear();
                                    },
                                    Err(e) => {
                                        // Check if this is a signal to skip the batch
                                        let error_msg = e.to_string();
                                        if error_msg.contains("SKIP_BATCH") {
                                            // Extract the next batch boundary ID if available
                                            let next_id = if let Some(id_str) = error_msg.split(':').nth(1) {
                                                id_str.parse::<i64>().unwrap_or(last_id + 1)
                                            } else {
                                                // If no specific ID provided, just skip current batch
                                                last_processed_id
                                            };
                                            
                                            println!("Skipping problematic batch and continuing with next batch boundary at ID: {}", next_id);
                                            
                                            // Update last_id to skip to the next batch boundary
                                            last_id = next_id;
                                            
                                            // Save updated ID to avoid reprocessing skipped records
                                            if let Err(e) = std::fs::write("last_processed_id.txt", last_id.to_string()) {
                                                println!("Warning: Failed to save last processed ID: {}", e);
                                            }
                                            
                                            // Clear the batch and break out of loop
                                            current_batch.clear();
                                            rows_count = 0;
                                            
                                            // Break out of the for loop entirely to restart with new last_id
                                            break;
                                        } else {
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
                            }
                        },
                        Err(e) => {
                            println!("Error processing row: {}", e);
                            
                            // Add counter for errors in the same batch
                            row_error_count += 1;
                            
                            // If too many errors in a row, force skip to next batch
                            if row_error_count >= max_row_errors_per_batch {
                                println!("Too many row errors ({}). Skipping to next batch.", row_error_count);
                                
                                // Calculate next batch boundary
                                let current_batch_start = (last_id / batch_size as i64) * batch_size as i64;
                                let next_batch_boundary = current_batch_start + batch_size as i64;
                                
                                println!("Skipping from ID {} to {}", last_id, next_batch_boundary);
                                
                                // Update last_id to skip ahead
                                last_id = next_batch_boundary;
                                
                                // Save the updated ID
                                if let Err(e) = std::fs::write("last_processed_id.txt", last_id.to_string()) {
                                    println!("Warning: Failed to save last processed ID: {}", e);
                                }
                                
                                // Reset error counter and set restart flag
                                row_error_count = 0;
                                current_batch.clear();
                                should_restart_query = true;
                                break;
                            }
                        }
                    }
                }
                
                if should_restart_query {
                    should_restart_query = false;
                    continue; // This will restart the entire while loop with the new last_id
                }

                // Process any remaining rows in a final batch
                if !current_batch.is_empty() {
                    match process_batch(&server_address, &current_batch, max_retries, retry_delay, last_id) {
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
                            // Check if this is a signal to skip the batch
                            let error_msg = e.to_string();
                            
                            if error_msg.contains("SKIP_BATCH") {
                                // Extract the next batch boundary ID if available
                                let next_id = if let Some(id_str) = error_msg.split(':').nth(1) {
                                    id_str.parse::<i64>().unwrap_or(last_id + 1)
                                } else {
                                    // If no specific ID provided, just skip current batch
                                    last_processed_id
                                };
                                
                                println!("FINAL BATCH: Skipping problematic batch and moving to next batch boundary at ID: {}", next_id);
                                
                                // Update last_id to skip to the next batch boundary
                                last_id = next_id;
                                
                                // Save updated ID to avoid reprocessing skipped records
                                if let Err(e) = std::fs::write("last_processed_id.txt", last_id.to_string()) {
                                    println!("Warning: Failed to save last processed ID: {}", e);
                                }
                                
                                // Clear the batch and exit this processing entirely
                                current_batch.clear();
                                rows_count = 0;
                                
                                println!("FINAL BATCH: Successfully updated last_id to: {}", last_id);
                                
                            } else {
                                // Connection failed, don't update last_id
                                println!("ERROR: Final batch processing failed, will retry data: {}", e);
                            }
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


fn process_batch(server_address: &str, batch: &Vec<SensorData>, max_retries: u32, retry_delay: u64, last_id: i64) -> Result<(), Box<dyn Error>> {
    // Build datapoints to match expected format
    let mut datapoints_json_array = Vec::with_capacity(batch.len());
    
    for sensor_data in batch {
        // Get the session ID as a string
        let _id_str = match sensor_data.session_id {
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

                let endpoint = "/sessions-sensors-data/batch";
                
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
                                
                                // Create error log entry
                                let timestamp = chrono::Local::now().format("%Y-%m-%d %H:%M:%S").to_string();
                                let error_log_path = "transmission_errors.txt";
                                
                                // Log the problematic batch details
                                let problematic_ids = batch.iter()
                                    .enumerate()
                                    .map(|(i, data)| format!("Record {}: Session {}", i, data.session_id.unwrap_or(0)))
                                    .collect::<Vec<String>>()
                                    .join(", ");
                                
                                // Error entry with batch information
                                let error_entry = format!(
                                    "\n[{}] ERROR: 400 Bad Request\n  Error details: {}\n  Batch size: {}\n  Records: {}\n  Sample data: {}\n  JSON preview: {}\n",
                                    timestamp,
                                    response.lines().last().unwrap_or("Unknown error"),
                                    batch.len(),
                                    problematic_ids,
                                    serde_json::to_string(&batch[0]).unwrap_or_default(),
                                    if json.len() > 200 { &json[0..200] } else { &json }
                                );
                                
                                // Write to error log
                                let mut file = match std::fs::OpenOptions::new()
                                    .create(true)
                                    .append(true)
                                    .open(error_log_path) {
                                        Ok(file) => file,
                                        Err(e) => {
                                            println!("Warning: Failed to open error log file: {}", e);
                                            return Err(Box::new(IoError::new(
                                                ErrorKind::InvalidData,
                                                format!("Bad request: {}", response.lines().last().unwrap_or("Unknown error"))
                                            )));
                                        }
                                    };
                                
                                if let Err(e) = file.write_all(error_entry.as_bytes()) {
                                    println!("Warning: Failed to write to error log: {}", e);
                                } else {
                                    println!("Error details appended to {}", error_log_path);
                                }
                                
                                // Increment attempts and check if should skip this batch
                                attempts += 1;
                                if attempts >= 3 {  // Skip after 3 attempts
                                    println!("SKIPPING: Failed to process batch after 3 attempts. Moving to next batch boundary.");
                                    
                                        // Find the starting ID of this batch
                                        let batch_start_id = batch.iter().enumerate()
                                            .filter_map(|(i, _)| {
                                                if i == 0 {
                                                    Some(last_id + 1)
                                                } else {
                                                    None
                                                }
                                            })
                                            .next()
                                            .unwrap_or(last_id + 1);
                                    
                                    // Calculate the next batch boundary
                                    let batch_size = batch.len() as i64;
                                    let next_batch_boundary = ((batch_start_id + batch_size - 1) / batch_size + 1) * batch_size;
                                    
                                    println!("Current batch starts at ID: {}, Moving to next batch boundary: {}", 
                                            batch_start_id, next_batch_boundary);
                                    
                                    // Return error type that indicates skip to the next batch boundary
                                    return Err(Box::new(IoError::new(
                                        ErrorKind::InvalidData,
                                        format!("SKIP_BATCH:{}", next_batch_boundary)
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
        if attempts >= max_retries {
        // Return a generic skip to next batch
        let next_batch_boundary = ((last_id / 100) + 1) * 100; // Assuming batch_size is 100
        return Err(Box::new(IoError::new(
            ErrorKind::InvalidData,
            format!("SKIP_BATCH:{}", next_batch_boundary)
        )));
    }
    
    Err(Box::new(IoError::new(
        ErrorKind::ConnectionAborted,
        format!("Failed to send batch after {} attempts", max_retries)
    )))
}


fn process_single_item(server_address: &str, sensor_data: &SensorData, max_retries: u32, retry_delay: u64) -> Result<(), Box<dyn Error>> {
    let mut attempts = 0;
    
    // Create the JSON for a single item
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
    
    // If no stored ID
    let second_row_id: i64 = conn.query_row(
        "SELECT IFNULL((SELECT rowid FROM sensor_data ORDER BY rowid LIMIT 1 OFFSET 1), 0)",
        params![],
        |row| row.get(0),
    )?;
    
    Ok(second_row_id)
}