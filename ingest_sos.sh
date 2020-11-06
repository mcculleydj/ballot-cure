#!/bin/bash

# first arg is the date e.g. 10-08, second is prod or dev:
# ./ingest.sh 10-08 dev

spawn () {
  if [[ $2 == "prod" ]]; then
    osascript -e 'tell app "Terminal" to do script "cd \"'$1'\"; python3 ingest_sos_chunk.py -d \"'$3'\" -c \"'$4'\" -p"'
  elif [[ $2 == "dev" ]]; then
    osascript -e 'tell app "Terminal" to do script "cd \"'$1'\"; python3 ingest_sos_chunk.py -d \"'$3'\" -c \"'$4'\""'
  fi
}

if [[ $2 == prod ]]; then
  read -r -p 'Are you sure you want to target production? [y/N] ' response
  if [[ "$response" =~ ^(y|Y)$ ]]; then
    echo 'Processing SoS CSV...'
    python3 process_sos_csv.py -d $1 -n 10 -p
    for i in {1..10}; do spawn $(pwd -P) $2 $1 $i; done
  fi
elif [[ $2 == dev ]]; then
  echo 'Processing SoS CSV...'
  python3 process_sos_csv.py -d $1 -n 10
  for i in {1..10}; do spawn $(pwd -P) $2 $1 $i; done
else
  echo 'Second argument must be prod or dev.'
fi
