# City Cycles Analytics Project

## Overview
This project provides an end-to-end analytics workflow for NYC CitiBike and London Santander bike-sharing data, using a cloud-native, modular architecture.

## Key Configuration Steps & Decisions

### Cloud Environment
- **Cloud Provider:** AWS
- **S3 Bucket:** `city-cycles-data-ctr37` (for raw and intermediate data)
- **RDS (Postgres):** `city-cycles-db` (database: `citycycles`)
- **EC2 Instance:** Ubuntu 22.04 LTS, t2.micro, 30 GiB EBS, IAM role for S3 access
- **SSH Key:** `city-cycles-ec2-key.pem`

### Python Environment
- **Python Version:** 3.13.3 (installed via deadsnakes PPA on Ubuntu)
- **Virtual Environment:** Created with `python3.13 -m venv venv`
- **Dependencies:** Installed from `requirements.txt`

### Project Structure
- Folders and files initialized as per `architecture.md` (see that file for details)
- All ETL, data ingestion, and transformation will be performed on the EC2 instance

### Networking & Security
- EC2 security group allows SSH from admin IP
- RDS security group allows inbound PostgreSQL (5432) from EC2 security group
- EC2 instance has IAM role for S3 access

### Decisions
- Use Ubuntu EC2 for easier Python and package management
- Use Python 3.13+ for compatibility with latest data tools and dbt
- Store all raw/intermediate data in S3, not on local disk
- All development and production runs will be cloud-native

## Setup Steps (So Far)
1. Create S3 bucket and IAM role for EC2
2. Provision RDS (Postgres) and test connectivity
3. Launch Ubuntu EC2, increase EBS to 30 GiB
4. Install Python 3.13, set up venv, clone repo, install dependencies
5. Test internet and RDS access from EC2

## Next Steps
- Configure secrets and environment variables
- Begin ETL and data ingestion tasks
- Update this README as more configuration and decisions are made

---

*This README will be updated as the project progresses and more tasks are completed.*
