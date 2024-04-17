#!/bin/bash

# Get the current month's first and last dates
start_date=$(date -d "$(date +%Y-%m-01) -1 month" +%Y-%m-01)
end_date=$(date -d "$start_date +1 month -1 day" +%Y-%m-%d)

# Run the Python script with start date and end date as arguments
python3 /path/to/extract_data.py "$start_date" "$end_date"
