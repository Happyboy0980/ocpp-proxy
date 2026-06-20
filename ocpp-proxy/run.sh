#!/usr/bin/with-contenv bashio

bashio::log.info "Starting OCPP Proxy..."

# Change to application directory and set PYTHONPATH
cd /app
export PYTHONPATH=/app/src

# Read port from add-on configuration
export PORT=$(bashio::config 'port')
bashio::log.info "Listening on port ${PORT}"

# Start the OCPP Proxy
exec python3 -m ocpp_proxy.main