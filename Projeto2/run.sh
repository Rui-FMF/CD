#!/bin/bash
DEFAULT_NUMBER_WORKERS=4

workers=${1:-$DEFAULT_NUMBER_WORKERS}

source venv/bin/activate
for i in $(seq 1 $workers); do
    x-terminal-emulator -e celery -A worker worker --loglevel=info -n worker$i@%h
done


x-terminal-emulator -e python3 server.py --max 15
sleep 8
curl -F 'video=moliceiro.m4v' http://localhost:5000
