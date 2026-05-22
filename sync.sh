#!/bin/bash
# Script helper para ejecutar la sincronización fácilmente

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

# Activar entorno virtual y ejecutar
.venv/bin/python3 src/main.py "$@"
