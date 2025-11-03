# EC2 Deployment Guide for City Cycles Pipeline

This guide covers deploying and configuring the City Cycles pipeline orchestrator on AWS EC2 for production use.

## Prerequisites

- AWS Account with EC2 access
- IAM user with S3 read/write permissions
- SSH key pair for EC2 access
- Domain name (optional, for monitoring)

## EC2 Instance Setup

### 1. Launch EC2 Instance

**Recommended Specifications:**
- **Instance Type:** `t3.xlarge` (4 vCPU, 16 GB RAM)
  - For larger datasets: `t3.2xlarge` (8 vCPU, 32 GB RAM)
- **OS:** Ubuntu 22.04 LTS
- **Storage:** 100 GB gp3 EBS volume
  - IOPS: 3000
  - Throughput: 125 MB/s
- **Security Group:**
  - Inbound: SSH (22) from your IP
  - Outbound: All traffic

**Launch Instance:**
```bash
aws ec2 run-instances \
  --image-id ami-0c7217cdde317cfec \
  --instance-type t3.xlarge \
  --key-name your-key-pair \
  --security-group-ids sg-xxxxxxxxx \
  --subnet-id subnet-xxxxxxxxx \
  --block-device-mappings '[{"DeviceName":"/dev/sda1","Ebs":{"VolumeSize":100,"VolumeType":"gp3","Iops":3000,"Throughput":125}}]' \
  --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=city-cycles-pipeline}]'
```

### 2. Configure IAM Role (Recommended)

Create IAM role with S3 access:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::city-cycles-data-ctr37",
        "arn:aws:s3:::city-cycles-data-ctr37/*",
        "arn:aws:s3:::tripdata",
        "arn:aws:s3:::tripdata/*"
      ]
    }
  ]
}
```

Attach role to EC2 instance (preferred over storing credentials).

## Initial Server Configuration

### 1. Connect to Instance

```bash
ssh -i your-key.pem ubuntu@your-ec2-ip
```

### 2. Update System

```bash
sudo apt update && sudo apt upgrade -y
```

### 3. Install System Dependencies

```bash
# Python and build tools
sudo apt install -y \
  python3.11 \
  python3.11-venv \
  python3.11-dev \
  python3-pip \
  build-essential \
  git \
  wget \
  curl \
  unzip

# Additional tools
sudo apt install -y \
  htop \
  tmux \
  vim \
  jq
```

### 4. Install Playwright Dependencies (for London extraction)

```bash
# Install Playwright system dependencies
sudo apt install -y \
  libnss3 \
  libnspr4 \
  libatk1.0-0 \
  libatk-bridge2.0-0 \
  libcups2 \
  libdrm2 \
  libxkbcommon0 \
  libxcomposite1 \
  libxdamage1 \
  libxfixes3 \
  libxrandr2 \
  libgbm1 \
  libasound2
```

## Project Deployment

### 1. Clone Repository

```bash
cd /home/ubuntu
git clone https://github.com/yourusername/city-cycles.git
cd city-cycles
```

### 2. Create Virtual Environment

```bash
python3.11 -m venv venv
source venv/bin/activate
```

### 3. Install Python Dependencies

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

### 4. Install Playwright Browsers

```bash
playwright install chromium
```

### 5. Create Directory Structure

```bash
# Create necessary directories
mkdir -p data logs

# Set permissions
chmod 755 data logs
```

## Configuration

### 1. Create Environment File

```bash
cat > .env << 'EOF'
# AWS Configuration
AWS_ACCESS_KEY_ID=your_access_key_here
AWS_SECRET_ACCESS_KEY=your_secret_key_here
AWS_DEFAULT_REGION=us-east-1
S3_BUCKET=city-cycles-data-ctr37

# DuckDB Configuration
DUCKDB_MEMORY_LIMIT=8GB
DUCKDB_THREADS=4

# Extraction Configuration
NYC_START_YEAR=2019
ENABLE_NYC_EXTRACTION=true
ENABLE_LONDON_EXTRACTION=true

# dbt Configuration
DBT_PROFILES_DIR=/home/ubuntu/.dbt
DBT_TARGET=prod

# Pipeline Configuration
DEFAULT_DBT_FULL_REFRESH=false
SKIP_EXTRACTION_ON_ERROR=false

# Logging
LOG_LEVEL=INFO
MAX_LOG_SIZE_MB=100
LOG_RETENTION_DAYS=30
EOF

# Secure the file
chmod 600 .env
```

### 2. Configure dbt Profile

```bash
mkdir -p ~/.dbt

cat > ~/.dbt/profiles.yml << 'EOF'
city_cycles:
  target: prod
  outputs:
    prod:
      type: duckdb
      path: /home/ubuntu/city-cycles/data/city_cycles.duckdb
      schema: main
      threads: 4
EOF
```

### 3. Validate Configuration

```bash
# Validate orchestrator config
python -m orchestrator.config --validate

# Test dbt connection
cd dbt_city_cycles
dbt debug
cd ..
```

## Initial Pipeline Run

### 1. Test Individual Stages

```bash
# Test extraction (will take 20-30 min)
python -m orchestrator.cli stage extraction

# Test file processing
python -m orchestrator.cli stage file_management

# Test database load
python -m orchestrator.cli stage database_load

# Test dbt (with full refresh for first run)
python -m orchestrator.cli stage dbt --full-refresh

# Test export
python -m orchestrator.cli stage export
```

### 2. Full Pipeline Run

```bash
# First complete run (expect 45-60 minutes)
python -m orchestrator.cli run --dbt-full-refresh

# Check status
python -m orchestrator.cli status
```

## Scheduling

### 1. Create Log Directory

```bash
sudo mkdir -p /var/log/city-cycles
sudo chown ubuntu:ubuntu /var/log/city-cycles
```

### 2. Create Cron Jobs

```bash
# Edit crontab
crontab -e

# Add these lines:

# Monthly pipeline run (1st of month at 2 AM)
0 2 1 * * cd /home/ubuntu/city-cycles && /home/ubuntu/city-cycles/venv/bin/python -m orchestrator.cli run >> /var/log/city-cycles/pipeline.log 2>&1

# Quarterly full refresh (1st of Jan/Apr/Jul/Oct at 3 AM)
0 3 1 1,4,7,10 * cd /home/ubuntu/city-cycles && /home/ubuntu/city-cycles/venv/bin/python -m orchestrator.cli run --dbt-full-refresh >> /var/log/city-cycles/pipeline-full.log 2>&1

# Weekly log cleanup (Sunday at 4 AM)
0 4 * * 0 find /var/log/city-cycles -name "*.log" -mtime +30 -delete

# Save and exit
```

### 3. Verify Cron Setup

```bash
# List cron jobs
crontab -l

# Check cron service
sudo systemctl status cron
```

## Monitoring and Maintenance

### 1. Check Pipeline Status

```bash
# Check orchestrator status
python -m orchestrator.cli status

# Check recent runs
tail -n 100 /var/log/city-cycles/pipeline.log

# Check for errors
grep ERROR /var/log/city-cycles/pipeline.log
```

### 2. Monitor System Resources

```bash
# Check disk usage
df -h

# Check memory usage
free -h

# Check CPU usage
htop

# Check DuckDB database size
du -h data/city_cycles.duckdb
```

### 3. Monitor S3 Usage

```bash
# Check S3 bucket size
aws s3 ls s3://city-cycles-data-ctr37 --recursive --summarize | grep "Total Size"

# Check recent uploads
aws s3 ls s3://city-cycles-data-ctr37/marts/ --recursive | tail -10
```

### 4. Set Up CloudWatch Monitoring (Optional)

```bash
# Install CloudWatch agent
wget https://s3.amazonaws.com/amazoncloudwatch-agent/ubuntu/amd64/latest/amazon-cloudwatch-agent.deb
sudo dpkg -i -E ./amazon-cloudwatch-agent.deb

# Configure to monitor:
# - CPU usage
# - Memory usage
# - Disk usage
# - Log files
```

## Backup Strategy

### 1. Database Backups

```bash
# Create backup script
cat > /home/ubuntu/backup_duckdb.sh << 'EOF'
#!/bin/bash
BACKUP_DIR="/home/ubuntu/city-cycles/data/backups"
DB_FILE="/home/ubuntu/city-cycles/data/city_cycles.duckdb"
DATE=$(date +%Y%m%d)

mkdir -p $BACKUP_DIR

# Create backup
cp $DB_FILE $BACKUP_DIR/city_cycles_$DATE.duckdb

# Compress backup
gzip $BACKUP_DIR/city_cycles_$DATE.duckdb

# Delete backups older than 90 days
find $BACKUP_DIR -name "*.duckdb.gz" -mtime +90 -delete

echo "Backup completed: city_cycles_$DATE.duckdb.gz"
EOF

chmod +x /home/ubuntu/backup_duckdb.sh

# Add to crontab (daily at 5 AM)
crontab -e
# Add: 0 5 * * * /home/ubuntu/backup_duckdb.sh >> /var/log/city-cycles/backup.log 2>&1
```

### 2. S3 Versioning

```bash
# Enable S3 versioning for data protection
aws s3api put-bucket-versioning \
  --bucket city-cycles-data-ctr37 \
  --versioning-configuration Status=Enabled
```

## Troubleshooting

### Common Issues

#### 1. Out of Memory Errors

```bash
# Reduce DuckDB memory limit
echo "DUCKDB_MEMORY_LIMIT=4GB" >> .env

# Reduce threads
echo "DUCKDB_THREADS=2" >> .env

# Or upgrade instance type
```

#### 2. Disk Space Issues

```bash
# Check disk usage
df -h

# Clean up old logs
sudo find /var/log/city-cycles -name "*.log" -mtime +30 -delete

# Clean up old backups
find /home/ubuntu/city-cycles/data/backups -name "*.gz" -mtime +90 -delete

# Clean dbt artifacts
cd /home/ubuntu/city-cycles/dbt_city_cycles
rm -rf target/ dbt_packages/
```

#### 3. Playwright/Browser Issues

```bash
# Reinstall Playwright
source /home/ubuntu/city-cycles/venv/bin/activate
playwright install --force chromium

# Check Playwright dependencies
playwright install-deps chromium
```

#### 4. AWS Credentials Issues

```bash
# Test S3 access
aws s3 ls s3://city-cycles-data-ctr37/

# If using IAM role, verify attachment
aws ec2 describe-instances --instance-ids i-xxxxxxxxx | grep IamInstanceProfile

# If using credentials, check .env file
cat /home/ubuntu/city-cycles/.env | grep AWS
```

### Recovery Procedures

#### Pipeline Failed Mid-Run

```bash
# Check which stage failed
tail -n 200 /var/log/city-cycles/pipeline.log

# Restart from failed stage
cd /home/ubuntu/city-cycles
source venv/bin/activate
python -m orchestrator.cli stage <failed_stage>

# Or restart full pipeline
python -m orchestrator.cli run
```

#### Database Corruption

```bash
# Restore from backup
cd /home/ubuntu/city-cycles/data
cp city_cycles.duckdb city_cycles.duckdb.corrupt
gunzip -c backups/city_cycles_YYYYMMDD.duckdb.gz > city_cycles.duckdb

# Or rebuild from S3
python -m db_duckdb.cli init
python -m db_duckdb.cli load
cd dbt_city_cycles
dbt run --full-refresh
```

## Security Best Practices

### 1. Use IAM Roles (Preferred)

Attach IAM role to EC2 instead of storing credentials.

### 2. Secure Credentials

```bash
# Ensure .env is secure
chmod 600 /home/ubuntu/city-cycles/.env

# Never commit .env to git
echo ".env" >> .gitignore
```

### 3. Update Security Group

```bash
# Only allow SSH from your IP
aws ec2 authorize-security-group-ingress \
  --group-id sg-xxxxxxxxx \
  --protocol tcp \
  --port 22 \
  --cidr your.ip.address/32
```

### 4. Regular Updates

```bash
# Set up unattended upgrades
sudo apt install unattended-upgrades
sudo dpkg-reconfigure -plow unattended-upgrades
```

## Performance Tuning

### For Larger Datasets

```bash
# Increase instance size
# t3.2xlarge (8 vCPU, 32 GB RAM)

# Increase DuckDB memory
echo "DUCKDB_MEMORY_LIMIT=24GB" >> .env

# Increase threads
echo "DUCKDB_THREADS=8" >> .env
```

### For Faster Runs

```bash
# Use faster storage
# Upgrade EBS volume to io2 with higher IOPS

# Optimize dbt
cd dbt_city_cycles
# Keep incremental strategy (default)
# Only use --full-refresh quarterly
```

## Cost Optimization

### EC2 Instance

```bash
# Use Savings Plans or Reserved Instances for 40-60% savings
# Stop instance when not in use (if not running cron jobs)

# Stop instance
aws ec2 stop-instances --instance-ids i-xxxxxxxxx

# Start instance
aws ec2 start-instances --instance-ids i-xxxxxxxxx
```

### S3 Storage

```bash
# Set up lifecycle policies for old files
aws s3api put-bucket-lifecycle-configuration \
  --bucket city-cycles-data-ctr37 \
  --lifecycle-configuration file://lifecycle.json

# lifecycle.json:
{
  "Rules": [
    {
      "Id": "archive-old-files",
      "Status": "Enabled",
      "Transitions": [
        {
          "Days": 90,
          "StorageClass": "INTELLIGENT_TIERING"
        }
      ]
    }
  ]
}
```

## Monitoring Dashboard (Optional)

### Set Up CloudWatch Dashboard

1. Go to CloudWatch console
2. Create new dashboard: "city-cycles-pipeline"
3. Add widgets:
   - EC2 CPU utilization
   - EC2 Memory usage
   - EBS read/write
   - S3 bucket size
   - Custom metrics from logs

### Set Up SNS Alerts

```bash
# Create SNS topic
aws sns create-topic --name city-cycles-alerts

# Subscribe email
aws sns subscribe \
  --topic-arn arn:aws:sns:us-east-1:ACCOUNT:city-cycles-alerts \
  --protocol email \
  --notification-endpoint your-email@example.com

# Create CloudWatch alarm for failed runs
# (Monitor log file for ERROR patterns)
```

## Maintenance Schedule

### Daily
- Automated: Cron runs (if scheduled daily)
- Automated: Log rotation

### Weekly
- Check disk space
- Review error logs
- Check S3 costs

### Monthly
- Automated: Pipeline run
- Review performance metrics
- Check for software updates

### Quarterly
- Automated: Full refresh run
- Review and optimize costs
- Security audit
- Update dependencies

## Checklist

### Pre-Deployment
- [ ] EC2 instance launched with correct specs
- [ ] IAM role created and attached
- [ ] Security group configured
- [ ] SSH key pair created
- [ ] S3 bucket created and accessible

### Deployment
- [ ] System updated
- [ ] Dependencies installed
- [ ] Project cloned
- [ ] Virtual environment created
- [ ] Python packages installed
- [ ] Playwright installed
- [ ] Configuration files created
- [ ] dbt profile configured

### Testing
- [ ] Configuration validated
- [ ] Individual stages tested
- [ ] Full pipeline run successful
- [ ] Data verified in DuckDB
- [ ] Marts exported to S3

### Production
- [ ] Cron jobs configured
- [ ] Logs directory created
- [ ] Monitoring set up
- [ ] Backup script created
- [ ] Documentation updated

## Support

For issues or questions:
1. Check logs: `/var/log/city-cycles/pipeline.log`
2. Review documentation: `/home/ubuntu/city-cycles/orchestrator/README.md`
3. Check pipeline status: `python -m orchestrator.cli status`
4. Contact: christophertrogers37@gmail.com

