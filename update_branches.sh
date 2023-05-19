#!/bin/bash

set -x

dolt remote add upstream dolthub/transparency-in-pricing
dolt checkout main
dolt pull upstream main

branches=("desert_springs" "henderson" "northwest" "nnmc" "southwest" "amc_wip" "bravera")

for branch in "${branches[@]}"; do
  echo "Checking out branch $branch..."
  dolt checkout "$branch"

  # Run 'dolt pull' and 'dolt push'
  echo "Running 'dolt merge main'..."
  dolt merge main
  echo "Running 'dolt push'..."
  dolt push
done
