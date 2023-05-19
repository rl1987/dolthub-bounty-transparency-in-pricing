#!/bin/bash

branches=("desert_springs" "henderson" "northwest" "nnmc" "southwest" "amc_wip" "bravera")

for branch in "${branches[@]}"; do
  echo "Checking out branch $branch..."
  dolt checkout "$branch"

  # Run 'dolt pull' and 'dolt push'
  echo "Running 'dolt pull upstream main'..."
  dolt pull upstream main
  echo "Running 'dolt push'..."
  dolt push
done
