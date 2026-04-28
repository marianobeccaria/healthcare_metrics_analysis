#!/bin/bash
set -e

echo "=== Installing Python packages ==="
su - ec2-user -c "pip3 install streamlit plotly pandas pyarrow \
    boto3 s3fs fsspec python-dotenv --user --quiet"

echo "=== Creating directories ==="
mkdir -p /home/ec2-user/dashboard
mkdir -p /home/ec2-user/.streamlit

echo "=== Creating Streamlit config ==="
cat > /home/ec2-user/.streamlit/config.toml << 'EOF'
[global]
showWarningOnDirectExecution = false
[logger]
level = "error"
[client]
showErrorDetails = false
EOF

echo "=== Downloading app.py from S3 ==="
python3 << 'PYEOF'
import sys
sys.path.insert(0, '/home/ec2-user/.local/lib/python3.9/site-packages')
import boto3
s3 = boto3.client('s3', region_name='us-east-1')
s3.download_file(
    'mbeccaria-dea-healthcare-metrics',
    'scripts/app.py',
    '/home/ec2-user/dashboard/app.py'
)
print("app.py downloaded successfully")
PYEOF

echo "=== Setting correct ownership ==="
chown -R ec2-user:ec2-user /home/ec2-user/dashboard
chown -R ec2-user:ec2-user /home/ec2-user/.streamlit

echo "=== Creating systemd service ==="
cat > /etc/systemd/system/streamlit.service << 'EOF'
[Unit]
Description=Healthcare Metrics Streamlit Dashboard
After=network.target
[Service]
User=ec2-user
WorkingDirectory=/home/ec2-user/dashboard
Environment=PATH=/home/ec2-user/.local/bin:/usr/local/bin:/usr/bin:/bin
Environment=HOME=/home/ec2-user
ExecStart=/home/ec2-user/.local/bin/streamlit run app.py --server.port 8501 --server.address 0.0.0.0
Restart=on-failure
RestartSec=5
[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable streamlit
systemctl start streamlit
echo "=== EC2 setup complete ==="
