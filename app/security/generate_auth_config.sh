#!/bin/bash
set -e
cd "$(dirname "$0")"
python3 generate_auth_config.py
