#!/bin/bash
# Revertimos a ejecutar el script python directamente para asegurarnos de que usa el sys.path hack
exec python server.py
