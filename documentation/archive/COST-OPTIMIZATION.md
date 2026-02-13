# Cost Optimization Guide

**Purpose:** Reduce AWS costs while maintaining portfolio-quality infrastructure.

---

## Current Cost Breakdown

### EC2 Instance (t3.xlarge)
| Component | Rate | Monthly (Always On) | Monthly (Optimized) |
|-----------|------|---------------------|---------------------|
| Compute | $0.1664/hr | $121.47 | ~$0.50 |
| EBS (100GB gp3) | $0.08/GB | $8.00 | $8.00 |
| **Subtotal** | | **$129.47** | **$8.50** |

### S3 Storage
| Component | Rate | Current | Notes |
|-----------|------|---------|-------|
| Storage (~80GB) | $0.023/GB | $1.84 | Minimal |
| PUT requests | $0.005/1000 | ~$0.05 | Monthly uploads |
| GET requests | $0.0004/1000 | ~$0.01 | Dashboard reads |
| Data transfer | $0.09/GB | ~$1.00 | To Streamlit |
| **Subtotal** | | **~$3.00** | |

### Current Total: ~$132/month

---

## Optimization Strategies

### Strategy 1: On-Demand EC2 (Recommended)

**Approach:** Only run EC2 during pipeline execution.

**Implementation:**

1. **Manual Start/Stop**
   ```bash
   # Before pipeline run
   aws ec2 start-instances --instance-ids i-xxxxxxxxx

   # Wait for running state
   aws ec2 wait instance-running --instance-ids i-xxxxxxxxx

   # SSH and run pipeline
   ssh -i key.pem ubuntu@<public-ip> "cd city-cycles && ./run_pipeline.sh"

   # Stop after completion
   aws ec2 stop-instances --instance-ids i-xxxxxxxxx
   ```

2. **Automated with Lambda + EventBridge**
   ```python
   # lambda/start_stop_ec2.py
   import boto3
   import os

   ec2 = boto3.client('ec2')
   INSTANCE_ID = os.environ['INSTANCE_ID']

   def handler(event, context):
       action = event.get('action', 'status')

       if action == 'start':
           ec2.start_instances(InstanceIds=[INSTANCE_ID])
           return {'status': 'starting'}
       elif action == 'stop':
           ec2.stop_instances(InstanceIds=[INSTANCE_ID])
           return {'status': 'stopping'}
       else:
           response = ec2.describe_instances(InstanceIds=[INSTANCE_ID])
           state = response['Reservations'][0]['Instances'][0]['State']['Name']
           return {'status': state}
   ```

   **EventBridge Schedule:**
   ```json
   {
     "schedule": "cron(55 1 1 * ? *)",
     "target": "lambda:start_ec2"
   }
   ```

**Cost Estimate:**
- Pipeline runtime: ~2 hours/month
- EC2 cost: 2 × $0.1664 = $0.33/month
- EBS cost: $8.00/month (charged even when stopped)
- **Total: ~$8.33/month**

### Strategy 2: Spot Instances

**Approach:** Use spot instances for 60-90% savings.

**Implementation:**

```bash
# Request spot instance
aws ec2 request-spot-instances \
  --instance-count 1 \
  --type "one-time" \
  --launch-specification file://spot-spec.json
```

```json
// spot-spec.json
{
  "ImageId": "ami-0c7217cdde317cfec",
  "InstanceType": "t3.xlarge",
  "KeyName": "city-cycles-key",
  "SecurityGroupIds": ["sg-xxxxxxxxx"],
  "UserData": "base64-encoded-startup-script"
}
```

**Cost Estimate:**
- Spot price: ~$0.05/hr (vs $0.1664 on-demand)
- 2 hours/month: $0.10/month
- **Total: ~$8.10/month**

**Trade-offs:**
- Can be interrupted (rare for t3 family)
- Need termination handling
- Acceptable for portfolio project

### Strategy 3: Smaller Instance

**Approach:** Downgrade instance size for routine runs.

| Instance | vCPU | RAM | Price | Use Case |
|----------|------|-----|-------|----------|
| t3.micro | 2 | 1GB | $0.0104/hr | Dashboard only |
| t3.small | 2 | 2GB | $0.0208/hr | Light processing |
| t3.medium | 2 | 4GB | $0.0416/hr | **Recommended** |
| t3.large | 2 | 8GB | $0.0832/hr | Full pipeline |
| t3.xlarge | 4 | 16GB | $0.1664/hr | Current (overkill) |

**Recommendation:** Use t3.medium for monthly incremental runs.

**Cost Estimate:**
- Hourly rate: $0.0416
- 2 hours/month: $0.08/month
- EBS: $8.00/month
- **Total: ~$8.08/month**

### Strategy 4: Local Development (Lowest Cost)

**Approach:** Run pipeline locally, use S3 only.

**Requirements:**
- Python 3.8+ environment
- DuckDB (runs natively on macOS/Linux/Windows)
- AWS CLI configured
- Playwright for London extraction

**Setup:**
```bash
# Clone repo
git clone https://github.com/chrisrogers37/city-cycles.git
cd city-cycles

# Create environment
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Configure AWS
aws configure

# Run pipeline
python -m orchestrator.cli run
```

**Cost Estimate:**
- S3 storage/transfer only: ~$3-5/month
- **Total: ~$3-5/month**

**Trade-offs:**
- Requires local compute resources
- No automated monthly runs
- Perfect for active development

### Strategy 5: GitHub Actions

**Approach:** Use GitHub Actions for pipeline execution.

```yaml
# .github/workflows/pipeline.yml
name: Monthly Pipeline

on:
  schedule:
    - cron: '0 2 1 * *'  # 1st of month at 2 AM UTC
  workflow_dispatch:  # Manual trigger

jobs:
  run-pipeline:
    runs-on: ubuntu-latest

    steps:
      - uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Install dependencies
        run: |
          pip install -r requirements.txt
          playwright install chromium

      - name: Configure AWS
        uses: aws-actions/configure-aws-credentials@v4
        with:
          aws-access-key-id: ${{ secrets.AWS_ACCESS_KEY_ID }}
          aws-secret-access-key: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
          aws-region: us-east-1

      - name: Run Pipeline
        run: python -m orchestrator.cli run --skip-verify
```

**Cost Estimate:**
- GitHub Actions: Free tier (2000 min/month)
- S3: ~$3-5/month
- **Total: ~$3-5/month**

**Trade-offs:**
- 6-hour job limit (may need to split)
- Less control over environment
- Good for showcase automation

---

## Recommended Configuration

### For Active Development

| Resource | Configuration | Monthly Cost |
|----------|---------------|--------------|
| EC2 | Stopped (local dev) | $0 |
| EBS | Keep 50GB for occasional use | $4 |
| S3 | Standard storage | $3-5 |
| Streamlit | Cloud (free tier) | $0 |
| **Total** | | **~$7-9** |

### For Portfolio Showcase (Automated)

| Resource | Configuration | Monthly Cost |
|----------|---------------|--------------|
| EC2 | t3.medium, on-demand, 2hr/month | $0.10 |
| EBS | 50GB gp3 | $4 |
| S3 | Standard storage | $3-5 |
| Lambda | Start/stop automation | $0 |
| Streamlit | Cloud (free tier) | $0 |
| **Total** | | **~$7-9** |

---

## Implementation Checklist

### Immediate Actions

- [ ] Stop EC2 instance when not in use
- [ ] Set up billing alerts ($10, $25, $50 thresholds)
- [ ] Reduce EBS volume to 50GB
- [ ] Document local development workflow

### Short-term (This Month)

- [ ] Create Lambda for EC2 start/stop
- [ ] Set up EventBridge schedule
- [ ] Test t3.medium for pipeline runs
- [ ] Update documentation

### Long-term (Next Quarter)

- [ ] Evaluate GitHub Actions for automation
- [ ] Consider spot instances for cost runs
- [ ] Implement S3 lifecycle policies
- [ ] Review and optimize S3 storage classes

---

## Billing Alerts Setup

```bash
# Create SNS topic for alerts
aws sns create-topic --name billing-alerts

# Subscribe email
aws sns subscribe \
  --topic-arn arn:aws:sns:us-east-1:ACCOUNT:billing-alerts \
  --protocol email \
  --notification-endpoint your-email@example.com

# Create CloudWatch alarm
aws cloudwatch put-metric-alarm \
  --alarm-name "Monthly-Cost-Exceeds-10" \
  --metric-name EstimatedCharges \
  --namespace AWS/Billing \
  --statistic Maximum \
  --period 21600 \
  --threshold 10 \
  --comparison-operator GreaterThanThreshold \
  --dimensions Name=Currency,Value=USD \
  --evaluation-periods 1 \
  --alarm-actions arn:aws:sns:us-east-1:ACCOUNT:billing-alerts
```

---

## S3 Lifecycle Policy

Reduce costs for infrequently accessed data:

```json
{
  "Rules": [
    {
      "ID": "Archive old parquet files",
      "Status": "Enabled",
      "Filter": {
        "Prefix": "extracted_bike_ride_parquet/"
      },
      "Transitions": [
        {
          "Days": 30,
          "StorageClass": "STANDARD_IA"
        },
        {
          "Days": 90,
          "StorageClass": "GLACIER_IR"
        }
      ]
    },
    {
      "ID": "Delete old ZIPs after processing",
      "Status": "Enabled",
      "Filter": {
        "Prefix": "extracted_bike_ride_zips/"
      },
      "Expiration": {
        "Days": 180
      }
    }
  ]
}
```

**Apply policy:**
```bash
aws s3api put-bucket-lifecycle-configuration \
  --bucket city-cycles-data-ctr37 \
  --lifecycle-configuration file://lifecycle.json
```

---

## Summary

| Strategy | Monthly Cost | Effort | Automation |
|----------|--------------|--------|------------|
| Current (Always On) | ~$132 | None | Full |
| On-Demand EC2 | ~$8-10 | Low | With Lambda |
| Spot Instances | ~$8 | Medium | With setup |
| Smaller Instance | ~$8 | Low | Full |
| Local Development | ~$3-5 | None | Manual |
| GitHub Actions | ~$3-5 | Medium | Full |

**Recommended Approach:**
1. Immediately stop EC2 when not in use (saves $120/month)
2. Develop locally, push to S3
3. Use Lambda + EventBridge for monthly automated runs
4. Keep Streamlit Cloud for free dashboard hosting

---

**Target Monthly Cost: <$10**

_Last updated: January 27, 2026_
