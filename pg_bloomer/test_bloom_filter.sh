#!/bin/bash

# Validate arguments
if [ "$#" -lt 2 ]; then
    echo "Usage: $0 <filter_id> <array_size> [query_size] [seed]"
    echo "  filter_id: The ID to use for the bloom filter query"
    echo "  array_size: Number of bytea elements to generate for the filter"
    echo "  query_size: Optional - Size of array for querying (defaults to array_size*2)"
    echo "  seed: Optional - Random seed for reproducibility"
    exit 1
fi

FILTER_ID=$1
ARRAY_SIZE=$2
QUERY_SIZE=${3:-$((ARRAY_SIZE * 2))}  # Default to twice the array size to test both hits and misses
SEED=${4:-$RANDOM}  # Use provided seed or generate a random one

# Set the seed for reproducibility
RANDOM=$SEED
echo "Using seed: $SEED" >&2

# Generate the SQL command to create the filter
echo "-- First create the Bloom filter with some items"
SQL_CREATE="SELECT * FROM new_bloom_filter_from_items(0.01, ARRAY[\n  "

# Generate the elements for filter creation
for ((i=0; i<ARRAY_SIZE; i++)); do
    # Generate a deterministic pattern for filter items
    BYTE1=$(printf "%02x" $((i % 256)))
    BYTE2=$(printf "%02x" $(((i+50) % 256)))
    BYTE3=$(printf "%02x" $(((i+100) % 256)))
    BYTE4=$(printf "%02x" $(((i+150) % 256)))
    
    # Add the element to the SQL command
    SQL_CREATE+="'\\\\x$BYTE1$BYTE2$BYTE3$BYTE4'"

    # Add comma if not the last element
    if [ $i -lt $((ARRAY_SIZE-1)) ]; then
        SQL_CREATE+=", "
        # Add newline every 4 elements for readability
        if [ $((i % 4)) -eq 3 ]; then
            SQL_CREATE+="\n  "
        fi  
    else
        SQL_CREATE+="\n"
    fi
    
done

# Complete the creation SQL command
SQL_CREATE+="]::bytea[]);"
echo -e "$SQL_CREATE"

SETUP_FILE="bloomf_test_setup_$ARRAY_SIZE.sql"
echo -e "$SQL_CREATE" > $SETUP_FILE
echo -e "\filter creation query written to $SETUP_FILE"

echo -e "\n-- Now query the filter with a mix of values (some will match, some won't)\n"
# Generate the query SQL command
SQL_QUERY="SELECT * FROM bloom_filter_contains_batch($FILTER_ID, ARRAY[\n  "

# Generate the elements for querying
for ((i=0; i<QUERY_SIZE; i++)); do
    # For half the items, use values that should be in the filter
    if [ $i -lt $((QUERY_SIZE / 2)) ] && [ $i -lt $ARRAY_SIZE ]; then
        # Use same pattern as during creation for matches
        BYTE1=$(printf "%02x" $((i % 256)))
        BYTE2=$(printf "%02x" $(((i+50) % 256)))
        BYTE3=$(printf "%02x" $(((i+100) % 256)))
        BYTE4=$(printf "%02x" $(((i+150) % 256)))
    else
        # Generate different patterns for non-matches
        BYTE1=$(printf "%02x" $(((i+200) % 256 )))
        BYTE2=$(printf "%02x" $(((i+210) % 256 )))
        BYTE3=$(printf "%02x" $(((i+220) % 256 )))
        BYTE4=$(printf "%02x" $(((i+230) % 256 )))
    fi
    
    # Add the element to the SQL command
    SQL_QUERY+="'\\\\x$BYTE1$BYTE2$BYTE3$BYTE4'"
    
    # Add comma if not the last element
    if [ $i -lt $((QUERY_SIZE-1)) ]; then
        SQL_QUERY+=", "
        # Add newline every 4 elements for readability
        if [ $((i % 4)) -eq 3 ]; then
            SQL_QUERY+="\n  "
        fi
    else 
        SQL_QUERY+="\n"
    fi

done

# Complete the query SQL command
SQL_QUERY+="]::bytea[]);"
echo -e "$SQL_QUERY"

# write sql to file
QUERY_FILE="bloomf_query_$ARRAY_SIZE.sql"
echo -e "$SQL_QUERY" > $QUERY_FILE
echo -e "\nquery written to $QUERY_FILE"

# Calculate the number of matches properly
if [ $QUERY_SIZE -lt $ARRAY_SIZE ]; then
  MATCH_COUNT=$((QUERY_SIZE / 2))
else
  MATCH_COUNT=$((ARRAY_SIZE / 2))
fi

echo -e "\n-- The first $MATCH_COUNT items should be true, the rest should be false"
