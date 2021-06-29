#!/bin/bash

PASS='confluent'

echo "Get a public and private key file to test the mTLS connection with"
openssl pkcs12 -in keystore/client.keystore.jks -out keystore/combined.key -passin pass:$PASS -passout pass:$PASS
echo "Showing public key"
openssl x509 -in keystore/combined.key -text -noout
echo "Test connection with client key"
openssl s_client -connect localhost:9093 -CAfile truststore/ca-cert -cert keystore/combined.key -state -debug -pass pass:$PASS