#!/bin/bash
set -e
echo "Testing: import awsio"
python -c "import awsio; print(awsio.__version__)"
echo "import awsio succeeded"

read -p "S3 URL : " s3_url
echo Testing: checking setup by quering whether or not $s3_url is an existing file
python -c "from awsio.python.lib.io.s3.s3dataset import file_exists; print(f\"file_exists: {file_exists($s3_url)}\")"
echo Smoke test was successful.

