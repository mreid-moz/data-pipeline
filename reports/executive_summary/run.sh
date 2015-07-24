#!/bin/bash

OUTPUT=output
TODAY=$(date +%Y%m%d)
if [ ! -d "$OUTPUT" ]; then
    mkdir -p "$OUTPUT/sandbox_preservation"
fi

# If we have an argument, process that day.
TARGET=$1
if [ -z "$TARGET" ]; then
  # Default to processing "yesterday"
  TARGET=$(date -d 'yesterday' +%Y%m%d)
fi

sed -r "s/__TARGET__/$TARGET/" schema_template.exec.json > schema.exec.json
sed -r "s/__TARGET__/$TARGET/" schema_template.crash.json > schema.crash.json

echo "Fetching previous state..."
aws s3 cp s3://telemetry-private-analysis/executive-report-v4/data/sandbox_preservation/FirefoxWeeklyDashboard.data.gz "$OUTPUT/sandbox_preservation/"

wget http://people.mozilla.org/~mreid/heka-minimal.tar.gz
tar xzvf heka-minimal.tar.gz

if [ -f "$OUTPUT/sandbox_preservation/FirefoxWeeklyDashboard.data.gz" ]; then
    # Back up previous state
    cp "$OUTPUT/sandbox_preservation/FirefoxWeeklyDashboard.data.gz" "$OUTPUT/sandbox_preservation/FirefoxWeeklyDashboard.data.prev.gz"
    gunzip "$OUTPUT/sandbox_preservation/FirefoxWeeklyDashboard.data.gz"
fi

heka-0_10_0-linux-amd64/bin/hekad -config exec.toml

echo "Compressing output"
gzip "$OUTPUT/sandbox_preservation/FirefoxWeeklyDashboard.data"
echo "Done!"

echo "Outputting to demo dashboard"
aws s3 cp "$OUTPUT/dashboard/data/FirefoxWeeklyDashboard.firefox_weekly_data.csv" s3://net-mozaws-prod-metrics-data/data-pipeline-demo/firefox_weekly_data.csv --grants full=emailaddress=mmayo@mozilla.com,emailaddress=cloudservices-aws-dev@mozilla.com,emailaddress=svcops-aws-dev@mozilla.com,emailaddress=svcops-aws-prod@mozilla.com
aws s3 cp "$OUTPUT/dashboard/data/FirefoxMonthlyDashboard.firefox_monthly_data.csv" s3://net-mozaws-prod-metrics-data/data-pipeline-demo/firefox_monthly_data.csv --grants full=emailaddress=mmayo@mozilla.com,emailaddress=cloudservices-aws-dev@mozilla.com,emailaddress=svcops-aws-dev@mozilla.com,emailaddress=svcops-aws-prod@mozilla.com
echo "Done!"
