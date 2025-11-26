#!/bin/bash

set -e

RED='\033[0;31m'
NC='\033[0m'  # Màu sắc mặc định
GREEN='\033[0;32m'

echo -e "\n\n${GREEN}** entrypoint.sh **${NC}"

# Run Uvicorn without SSL certificates on port 80
exec uvicorn main:app --host 0.0.0.0 --port 80