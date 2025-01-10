# Apache Airflow Setup Guide

This guide covers setting up Apache Airflow both as a systemd service and with manual startup options.

## Directory Structure
```bash
/home/abhinav/airflow/          # Main Airflow directory
├── airflow/                    # Virtual environment directory
│   └── bin/                   # Contains activation script and airflow executable
└── logs/                      # Logs directory
    ├── webserver.log
    ├── webserver.error.log
    ├── scheduler.log
    └── scheduler.error.log
```

## Installation and Initial Setup

1. Create and activate virtual environment:
```bash
cd /home/abhinav/airflow
python3 -m venv airflow
source airflow/bin/activate
```

2. Install Airflow:
```bash
pip install apache-airflow
```

## Method 1: Systemd Service Setup (Recommended)

### 1. Create Webserver Service
```bash
sudo vi /etc/systemd/system/airflow-webserver.service
```

Content:
```ini
[Unit]
Description=Airflow webserver daemon
After=network.target postgresql.service mysql.service redis.service rabbitmq-server.service
Wants=postgresql.service mysql.service redis.service rabbitmq-server.service

[Service]
Environment="AIRFLOW_HOME=/home/abhinav/airflow"
User=abhinav
Group=abhinav
Type=simple
WorkingDirectory=/home/abhinav/airflow
ExecStart=/home/abhinav/airflow/airflow/bin/airflow webserver
Restart=always
RestartSec=5s
StandardOutput=append:/home/abhinav/airflow/logs/webserver.log
StandardError=append:/home/abhinav/airflow/logs/webserver.error.log

[Install]
WantedBy=multi-user.target
```

### 2. Create Scheduler Service
```bash
sudo vi /etc/systemd/system/airflow-scheduler.service
```

Content:
```ini
[Unit]
Description=Airflow scheduler daemon
After=network.target postgresql.service mysql.service redis.service rabbitmq-server.service
Wants=postgresql.service mysql.service redis.service rabbitmq-server.service

[Service]
Environment="AIRFLOW_HOME=/home/abhinav/airflow"
User=abhinav
Group=abhinav
Type=simple
WorkingDirectory=/home/abhinav/airflow
ExecStart=/home/abhinav/airflow/airflow/bin/airflow scheduler
Restart=always
RestartSec=5s
StandardOutput=append:/home/abhinav/airflow/logs/scheduler.log
StandardError=append:/home/abhinav/airflow/logs/scheduler.error.log

[Install]
WantedBy=multi-user.target
```

### 3. Setup and Enable Services
```bash
# Create logs directory
mkdir -p /home/abhinav/airflow/logs

# Set correct permissions
sudo chown -R abhinav:abhinav /home/abhinav/airflow
sudo chmod 755 /home/abhinav/airflow/logs

# Enable and start services
sudo systemctl daemon-reload
sudo systemctl enable airflow-webserver
sudo systemctl enable airflow-scheduler
sudo systemctl start airflow-webserver
sudo systemctl start airflow-scheduler
```

### Service Management Commands
```bash
# Check status
sudo systemctl status airflow-webserver
sudo systemctl status airflow-scheduler

# Stop services
sudo systemctl stop airflow-webserver
sudo systemctl stop airflow-scheduler

# Restart services
sudo systemctl restart airflow-webserver
sudo systemctl restart airflow-scheduler

# View logs
sudo journalctl -u airflow-webserver
sudo journalctl -u airflow-scheduler
```

## Method 2: Manual Startup via .bashrc

Add the following to your ~/.bashrc:

```bash
# Airflow configuration
export AIRFLOW_HOME=/home/abhinav/airflow

# Function to check if process is running
is_running() {
    pgrep -f "$1" >/dev/null
}

# Function to start Airflow components
start_airflow() {
    # Change to correct directory
    cd /home/abhinav/airflow
    
    # Activate virtual environment
    source airflow/bin/activate
    
    if ! is_running "airflow webserver"; then
        echo "Starting Airflow webserver..."
        nohup airflow webserver > $AIRFLOW_HOME/logs/webserver.log 2>&1 &
    fi
    
    if ! is_running "airflow scheduler"; then
        echo "Starting Airflow scheduler..."
        nohup airflow scheduler > $AIRFLOW_HOME/logs/scheduler.log 2>&1 &
    fi
}

# Add convenience alias
alias airflow-start='start_airflow'

# Add convenience function to switch to airflow environment
airflow-env() {
    cd /home/abhinav/airflow
    source airflow/bin/activate
    echo "Airflow environment activated"
}
```

### Alternative Script Approach

Create ~/start-airflow.sh:
```bash
#!/bin/bash

export AIRFLOW_HOME=/home/abhinav/airflow

# Change to airflow directory
cd /home/abhinav/airflow

# Activate virtual environment
source airflow/bin/activate

# Kill existing processes if any
pkill -f "airflow webserver"
pkill -f "airflow scheduler"

# Start new processes
nohup airflow webserver > $AIRFLOW_HOME/logs/webserver.log 2>&1 &
nohup airflow scheduler > $AIRFLOW_HOME/logs/scheduler.log 2>&1 &
```

Make it executable:
```bash
chmod +x ~/start-airflow.sh
```

Add to ~/.bashrc for automatic startup:
```bash
# Start Airflow if not running
if ! pgrep -f "airflow webserver" > /dev/null; then
    ~/start-airflow.sh
fi

# Add shortcut to switch to airflow environment
alias airflow-env='cd /home/abhinav/airflow && source airflow/bin/activate'
```

After making changes:
```bash
source ~/.bashrc
```

## Usage
- Use `airflow-start` to manually start Airflow
- Use `airflow-env` to quickly switch to the Airflow environment
- Access the Airflow UI at http://localhost:8080

## Notes
- The systemd service approach (Method 1) is recommended for production-like environments
- The .bashrc approach (Method 2) is more suitable for development environments
- Logs are stored in /home/abhinav/airflow/logs/
- Make sure to adjust permissions if you encounter any access issues