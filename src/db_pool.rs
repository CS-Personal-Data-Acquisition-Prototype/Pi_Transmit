use rusqlite::{Connection, Error as SQLiteError};
use std::sync::{Arc, Mutex};
use std::collections::VecDeque;
use std::time::Duration;
use std::thread;
use std::path::Path;

pub struct ConnectionPool {
    connections: Arc<Mutex<VecDeque<Connection>>>,
    db_path: String,
    max_size: usize,
    timeout: Duration,
    busy_timeout: Duration,
}

impl ConnectionPool {
    pub fn new<P: AsRef<Path>>(db_path: P, max_size: usize, timeout_ms: u64, busy_timeout_ms: u64) -> Result<Self, SQLiteError> {
        let db_path_str = db_path.as_ref().to_string_lossy().to_string();
        let mut connections = VecDeque::with_capacity(max_size);

        // Initialize with a smaller number of connections initially
        let initial_size = std::cmp::min(2, max_size);
        for _ in 0..initial_size {
            let conn = Self::create_connection(&db_path_str, busy_timeout_ms)?;
            connections.push_back(conn);
        }

        Ok(ConnectionPool {
            connections: Arc::new(Mutex::new(connections)),
            db_path: db_path_str,
            max_size,
            timeout: Duration::from_millis(timeout_ms),
            busy_timeout: Duration::from_millis(busy_timeout_ms),
        })
    }

    fn create_connection(db_path: &str, busy_timeout_ms: u64) -> Result<Connection, SQLiteError> {
        let conn = Connection::open(db_path)?;
        
        // Set busy timeout to handle database locks
        conn.busy_timeout(Duration::from_millis(busy_timeout_ms))?;
        
        // Enable WAL mode for better concurrency
        conn.execute_batch("PRAGMA journal_mode = WAL")?;
        
        // Other pragmas that can help with concurrency
        conn.execute_batch("PRAGMA synchronous = NORMAL")?;
        
        Ok(conn)
    }

    pub fn get(&self) -> Result<PooledConnection, SQLiteError> {
        let start_time = std::time::Instant::now();
        
        loop {
            // Try to get a connection from the pool
            let mut guard = self.connections.lock().unwrap();
            
            if let Some(conn) = guard.pop_front() {
                drop(guard); // Release the lock
                return Ok(PooledConnection {
                    conn: Some(conn),
                    pool: self.clone(),
                });
            }
            
            // If pool isn't at max capacity, create a new connection
            if guard.len() < self.max_size {
                drop(guard); // Release the lock before potentially slow operation
                
                match Self::create_connection(&self.db_path, self.busy_timeout.as_millis() as u64) {
                    Ok(conn) => {
                        println!("Created new connection in pool (current size: {})", self.connections.lock().unwrap().len() + 1);
                        return Ok(PooledConnection {
                            conn: Some(conn),
                            pool: self.clone(),
                        });
                    },
                    Err(e) => {
                        println!("Error creating new connection: {}", e);
                        return Err(e);
                    }
                }
            }
            
            // If we've been waiting too long, return an error
            if start_time.elapsed() > self.timeout {
                return Err(SQLiteError::ExecuteReturnedResults);
            }
            
            // Otherwise wait a bit and try again
            drop(guard);
            thread::sleep(Duration::from_millis(100));
        }
    }

    fn return_connection(&self, conn: Connection) {
        let mut guard = self.connections.lock().unwrap();
        
        // Check if the connection is valid before returning to pool
        let valid = conn.execute_batch("SELECT 1").is_ok();
        
        if valid && guard.len() < self.max_size {
            guard.push_back(conn);
        }
        // If connection is invalid or pool is full, it will be dropped and closed
    }
}

impl Clone for ConnectionPool {
    fn clone(&self) -> Self {
        ConnectionPool {
            connections: Arc::clone(&self.connections),
            db_path: self.db_path.clone(),
            max_size: self.max_size,
            timeout: self.timeout,
            busy_timeout: self.busy_timeout,
        }
    }
}

pub struct PooledConnection {
    conn: Option<Connection>,
    pool: ConnectionPool,
}

impl Drop for PooledConnection {
    fn drop(&mut self) {
        if let Some(conn) = self.conn.take() {
            self.pool.return_connection(conn);
        }
    }
}

impl std::ops::Deref for PooledConnection {
    type Target = Connection;
    
    fn deref(&self) -> &Self::Target {
        self.conn.as_ref().unwrap()
    }
}

impl std::ops::DerefMut for PooledConnection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.conn.as_mut().unwrap()
    }
}