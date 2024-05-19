#!/bin/bash

# Create a temporary file to store the responses
response_file=$(mktemp)
echo "Response file: $response_file"

num_requests=100

# Initialize arrays to store the times
declare -a times

# Function to make a single POST request
make_request() {
    # Calculate the time taken to make the request
    local start_time=$(date +%s%N)
    curl -X POST http://dist-proj-api.mu-stafa.com/api/v1/image_processing \
        -F "images=@img1.png" \
        -F "images=@img2.jpg" \
        -F "images=@img3.jpg" \
        -F "operation=BLUR" \
        -o $response_file --no-buffer --http2
    local end_time=$(date +%s%N)
    local duration=$((end_time - start_time))
    echo $duration
}

export -f make_request
export response_file

for i in $(seq $num_requests); do
    times+=($(make_request))
done

# Calculate the total, average, max, and min times
total_time=0
max_time=0
min_time=${times[0]}

for time in "${times[@]}"; do
    total_time=$((total_time + time))
    if [[ $time -gt $max_time ]]; then
        max_time=$time
    fi
    if [[ $time -lt $min_time ]]; then
        min_time=$time
    fi
done

average_time=$((total_time / num_requests))

# Echo the average, maximum, and minimum times in ms
echo "Average time taken per request (3 images): $(($average_time / 1000000)) ms"
echo "Maximum time taken per request (3 images): $(($max_time / 1000000)) ms"
echo "Minimum time taken per request (3 images): $(($min_time / 1000000)) ms"
echo "Average time taken per image: $(($average_time / 1000000 / 3)) ms"

# Clean up
rm $response_file
