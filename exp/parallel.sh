#!/bin/bash

# Define configurations
declare -a ports=(5010 5020 5030 5040 5050 5060 5070 5080)
declare -a left_sizes=(292 368 298 113 99 94 61 23)
declare -a right_sizes=(16 37 52 46 55 61 69 76)
declare -a compaction_sizes=(16 37 52 46 55 61 69 76)

# Start all pairs of joins concurrently and record their PIDs
pids=()

echo "Starting all join pairs..."
start_time=$(date +%s.%N)  # Record the start time in seconds with nanosecond precision

for i in "${!ports[@]}"; do
  # Start the first party
  ./bin/exp_parallel_join 1 "${ports[$i]}" "${left_sizes[$i]}" "${right_sizes[$i]}" "${compaction_sizes[$i]}" &
  pids+=($!)
  
  # Start the second party
  ./bin/exp_parallel_join 2 "${ports[$i]}" "${left_sizes[$i]}" "${right_sizes[$i]}" "${compaction_sizes[$i]}" &
  pids+=($!)
done

# Wait for all processes to complete
for pid in "${pids[@]}"; do
    wait $pid
done

end_time=$(date +%s.%N)  # Record the end time in seconds with nanosecond precision
elapsed=$(echo "($end_time - $start_time) * 1000" | bc)  # Calculate elapsed time in milliseconds

echo "All processes have completed."
echo "Total elapsed time: $elapsed milliseconds."
