#!/bin/bash

PROTOC_VERSION="29.2"
PROTOC_ZIP="protoc-${PROTOC_VERSION}-linux-x86_64.zip"
PROTOC_URL="https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/${PROTOC_ZIP}"
PROTOC_DIR="./protobuf/compiler"

# Check if protoc exists in the compiler directory
if [ ! -f "${PROTOC_DIR}/bin/protoc" ]; then
    echo "protoc not found in ${PROTOC_DIR}, downloading..."
    
    # Create compiler directory if it doesn't exist
    mkdir -p "$PROTOC_DIR"
    
    # Download protoc
    if ! curl -L "$PROTOC_URL" -o "${PROTOC_DIR}/${PROTOC_ZIP}"; then
        echo "Failed to download protoc"
        exit 1
    fi
    
    # Extract the archive
    if ! unzip -o "${PROTOC_DIR}/${PROTOC_ZIP}" -d "$PROTOC_DIR"; then
        echo "Failed to extract protoc"
        exit 1
    fi
    
    # Cleanup zip file
    rm "${PROTOC_DIR}/${PROTOC_ZIP}"
    
    echo "protoc installed successfully in ${PROTOC_DIR}"
fi

# Compile protobuf files
echo "Compiling protobuf files..."
for proto_file in ./protobuf/src/*.proto; do
    if [ -f "$proto_file" ]; then
        echo "Compiling $proto_file"
        "${PROTOC_DIR}/bin/protoc" \
            --proto_path=./protobuf/src \
            --python_out=./src/protobuf \
            "$proto_file"
    fi
done

echo "Protobuf compilation complete"
