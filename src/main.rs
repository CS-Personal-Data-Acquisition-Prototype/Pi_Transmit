use rusqlite::{params, Connection};
use std::error::Error;
use std::io::Write;
use std::net::TcpStream;

fn main() -> Result<(), Box<dyn Error>> {
    // 1. Open the local SQLite database
    let conn = Connection::open("../Pi_TCP/src/data_acquisition.db")?;

    // 2. Query all rows from sensor_data
    let mut stmt = conn.prepare("SELECT 
        sessionID, timestamp, latitude, longitude, altitude, 
        accel_x, accel_y, accel_z, 
        gyro_x, gyro_y, gyro_z, 
        dac_1, dac_2, dac_3, dac_4
        FROM sensor_data")?;

    // 3. Connect to the remote server (change IP & port as needed)
    let mut stream = TcpStream::connect("0.0.0.0:9000")?;
    println!("Connected to remote server.");

    // 4. Send records as CSV lines
    let rows = stmt.query_map(params![], |row| {
        // Pull columns out in the correct order
        Ok(format!(
            "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
            row.get::<_, Option<i32>>(0)?.map_or("None".to_string(), |v| v.to_string()),
            row.get::<_, Option<String>>(1)?.unwrap_or_else(|| "None".to_string()),
            row.get::<_, f64>(2)?,
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
        ))
    })?;

    for row_result in rows {
        let csv_line = row_result?;
        // Send each row followed by newline
        stream.write_all(csv_line.as_bytes())?;
        stream.write_all(b"\n")?;
    }

    println!("All rows sent successfully.");
    Ok(())
}