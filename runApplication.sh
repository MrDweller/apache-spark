#!/bin/bash

# Check if a filename was provided
if [ $# -ne 1 ]; then
  echo "Usage: $0 <filename>"
  echo "The given file must be located in ./applications"
  exit 1
fi

# Check if the file exists in ./applications
filename="$1"
filepath="./applications/$filename"

if [ ! -f "$filepath" ]; then
  echo "Error: File '$filepath' does not exist."
  exit 1
fi

# Run the docker command
docker exec spark-master spark-submit "$filepath"

echo "Submitted '$filename' to Spark successfully."