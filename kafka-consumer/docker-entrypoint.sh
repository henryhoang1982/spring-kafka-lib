#!/bin/sh
set -e

# Create a temporary directory in ephemeral storage
CERTS_DIR="/tmp/kafka-certs"
mkdir -p $CERTS_DIR
chmod 700 $CERTS_DIR

# Write keystores to ephemeral storage
echo "$truststore" | base64 -d > $CERTS_DIR/truststore.jks
echo "$keystore" | base64 -d > $CERTS_DIR/keystore.jks

# Set appropriate permissions
chmod 600 $CERTS_DIR/truststore.jks $CERTS_DIR/keystore.jks

# Execute the main application
exec java \
  -Djavax.net.ssl.trustStore=$CERTS_DIR/truststore.jks \
  -Djavax.net.ssl.keyStore=$CERTS_DIR/keystore.jks \
  -Djavax.net.ssl.trustStorePassword=${SSL_PASSWORD} \
  -Djavax.net.ssl.keyStorePassword=${SSL_PASSWORD} \
  ${JAVA_OPTS} \
  -jar app.jar 