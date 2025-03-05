# Pi_Transmit

A utility for transmitting database data from a Raspberry Pi to a remote server automatically.

## Overview

Pi_Transmit is designed to facilitate the seamless transfer of database information from a Raspberry Pi device to a designated server. This tool is particularly useful for IoT applications, remote monitoring systems, and data collection scenarios where periodic data uploads are required.

## Features

- Automated database synchronization
- Configurable transfer schedules
- Secure data transmission
- Error handling and retry mechanisms
- Logging of transfer activities and errors

## Setup

1. Clone this repository to your Raspberry Pi
2. Install required dependencies
3. Configure the `config.ini` file (see Configuration section)
4. Run the main script to start transmission

## Configuration

The `config.ini` file contains essential settings for the application. Create this file in the root directory with the following information:

```ini
[Database]
# Local database settings
db_type = sqlite  # or mysql, postgresql, etc.
db_path = /path/to/your/database.db
table_name = data_table

[Server]
# Remote server connection details
server_url = https://example.com/api/endpoint
api_key = your_api_key_here
username = server_username
password = server_password

[Transfer]
# Transfer configuration
interval = 3600  # Transfer interval in seconds
retry_attempts = 3
retry_delay = 300  # Delay between retry attempts in seconds

[Logging]
# Logging configuration
log_level = INFO  # DEBUG, INFO, WARNING, ERROR, CRITICAL
log_file = /path/to/logfile.log
```

## Usage

To start the data transmission process:

```bash
python transmit.py
```

For running as a background service:

```bash
nohup python transmit.py > output.log &
```

## Troubleshooting

Check the log file for detailed information about any errors encountered during transmission. Common issues include:
- Network connectivity problems
- Database access errors
- Server authentication failures

## License Notice
To apply the Apache License to your work, attach the following boilerplate notice. The text should be enclosed in the appropriate comment syntax for the file format. We also recommend that a file or class name and description of purpose be included on the same "printed page" as the copyright notice for easier identification within third-party archives.

    Copyright 2025 CS 462 Personal Data Acquisition Prototype Group
    
    Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
    
    http://www.apache.org/licenses/LICENSE-2.0
    Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
