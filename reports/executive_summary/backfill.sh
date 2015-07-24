#!/bin/bash

OUTPUT=output
TODAY=$(date +%Y%m%d)
if [ ! -d "$OUTPUT" ]; then
    mkdir -p "$OUTPUT/sandbox_preservation"
fi
wget http://people.mozilla.org/~mreid/heka-minimal.tar.gz
tar xzvf heka-minimal.tar.gz

for n in $(seq 7 -1 1); do
    D=$(date -d "$n days ago" +%Y%m%d)
    echo "Processing $D"
    sed -r "s/__TARGET__/$D/" schema_template.json > schema.json
    heka-0_10_0-linux-amd64/bin/hekad -config exec.toml
    echo "Done with $D"
done

echo "Compressing output"
gzip "$OUTPUT/sandbox_preservation/FirefoxWeeklyDashboard.data"
echo "Done!"

echo "Outputting to demo dashboard"
aws s3 cp "$OUTPUT/dashboard/data/FirefoxWeeklyDashboard.firefox_weekly_data.csv" s3://net-mozaws-prod-metrics-data/data-pipeline-demo/firefox_weekly_data.csv
aws s3 cp "$OUTPUT/dashboard/data/FirefoxMonthlyDashboard.firefox_monthly_data.csv" s3://net-mozaws-prod-metrics-data/data-pipeline-demo/firefox_monthly_data.csv
echo "Done!"
