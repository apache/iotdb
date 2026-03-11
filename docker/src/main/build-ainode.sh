#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
#
# AINode Docker Image Build Script
# Run this script from docker/src/main directory
# Usage: ./build.sh -v <version> [options]
#

set -e

# Get script directory (should be docker/src/main)
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# Project root is 3 levels up from docker/src/main
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"

# Default configuration
VERSION=""
IMAGE_NAME="apache/iotdb"
IMAGE_TAG_SUFFIX="ainode"
PUSH_IMAGE=false
NO_CACHE=false
DATA_DIR="/data/ainode"
REGISTRY_PREFIX=""

# Usage information
usage() {
    cat << EOF
Usage: $0 -v <version> [options]

Required:
    -v, --version <version>     Specify IoTDB version (e.g., 1.0.0, 2.0.1)

Options:
    -p, --push                  Push image to registry after build
    -n, --no-cache              Build without Docker cache
    -t, --tag <tag>             Custom image tag (default: <version>-ainode)
    -d, --data-dir <path>       Data directory path (default: /data/ainode)
    -r, --registry <url>        Registry prefix (e.g., registry.example.com)
    -h, --help                  Show this help message

Examples:
    # Build version 1.0.0
    $0 -v 1.0.0

    # Build and push
    $0 -v 1.0.0 --push

    # Build with custom data directory
    $0 -v 1.0.0 --data-dir /mnt/data/ainode

    # Build for private registry
    $0 -v 2.0.1 --registry registry.example.com/apache --push
EOF
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -v|--version)
            VERSION="$2"
            shift 2
            ;;
        -p|--push)
            PUSH_IMAGE=true
            shift
            ;;
        -n|--no-cache)
            NO_CACHE=true
            shift
            ;;
        -t|--tag)
            CUSTOM_TAG="$2"
            shift 2
            ;;
        -d|--data-dir)
            DATA_DIR="$2"
            shift 2
            ;;
        -r|--registry)
            REGISTRY_PREFIX="$2"
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            usage
            exit 1
            ;;
    esac
done

# Validate version
if [ -z "$VERSION" ]; then
    echo "Error: Version is required. Use -v <version> to specify."
    usage
    exit 1
fi

# Check if distribution package exists (relative to project root)
DIST_FILE="${PROJECT_ROOT}/distribution/target/apache-iotdb-${VERSION}-ainode-bin.zip"
if [ ! -f "$DIST_FILE" ]; then
    echo "Error: Distribution file not found: $DIST_FILE"
    echo "Please build the project first: mvn clean package -DskipTests"
    exit 1
fi

# Check if data directory exists
if [ ! -d "$DATA_DIR" ]; then
    echo "Warning: Data directory does not exist: $DATA_DIR"
    echo "Creating empty directory..."
    mkdir -p "$DATA_DIR"
fi

# Determine image tag
if [ -n "$CUSTOM_TAG" ]; then
    IMAGE_TAG="${CUSTOM_TAG}"
else
    IMAGE_TAG="${VERSION}-${IMAGE_TAG_SUFFIX}"
fi

# Construct full image name
if [ -n "$REGISTRY_PREFIX" ]; then
    FULL_IMAGE_NAME="${REGISTRY_PREFIX}/${IMAGE_NAME}:${IMAGE_TAG}"
else
    FULL_IMAGE_NAME="${IMAGE_NAME}:${IMAGE_TAG}"
fi

echo "============================================"
echo "Building AINode Docker Image"
echo "============================================"
echo "Version:        ${VERSION}"
echo "Distribution:   ${DIST_FILE}"
echo "Data Directory: ${DATA_DIR}"
echo "Dockerfile:     ${SCRIPT_DIR}/Dockerfile-2.0.7-ainode"
echo "Image Name:     ${FULL_IMAGE_NAME}"
echo "Build Context:  ${PROJECT_ROOT}"
echo "============================================"

# Prepare temporary data directory for Docker build context
# Docker cannot COPY files from absolute paths outside build context
TMP_DATA_DIR="${SCRIPT_DIR}/tmp-data"
echo "Preparing data directory for build context..."

# Clean up old temp data if exists
if [ -d "$TMP_DATA_DIR" ]; then
    rm -rf "$TMP_DATA_DIR"
fi

# Copy data to temporary location within build context
mkdir -p "$TMP_DATA_DIR"
if [ -d "$DATA_DIR" ] && [ "$(ls -A $DATA_DIR)" ]; then
    cp -r "$DATA_DIR"/* "$TMP_DATA_DIR/"
    echo "Copied data from ${DATA_DIR} to ${TMP_DATA_DIR}"
else
    echo "No data to copy, creating empty directory"
fi

# Ensure cleanup on exit
cleanup() {
    echo "Cleaning up temporary data directory..."
    rm -rf "$TMP_DATA_DIR"
}
trap cleanup EXIT

# Build Docker image
# Build context is PROJECT_ROOT (3 levels up from current script)
BUILD_CMD="docker build"
BUILD_CMD+=" --file ${SCRIPT_DIR}/Dockerfile-2.0.7-ainode"
BUILD_CMD+=" --build-arg VERSION=${VERSION}"
BUILD_CMD+=" --build-arg BUILD_DATE=$(date -u +'%Y-%m-%dT%H:%M:%SZ')"
BUILD_CMD+=" --build-arg VCS_REF=$(git rev-parse --short HEAD 2>/dev/null || echo 'unknown')"

if [ "$NO_CACHE" = true ]; then
    BUILD_CMD+=" --no-cache"
fi

BUILD_CMD+=" --tag ${FULL_IMAGE_NAME}"
BUILD_CMD+=" ${PROJECT_ROOT}"

echo "Executing: ${BUILD_CMD}"
${BUILD_CMD} || {
    echo "Error: Docker build failed"
    exit 1
}

echo ""
echo "Build completed successfully: ${FULL_IMAGE_NAME}"

# Push image if requested
if [ "$PUSH_IMAGE" = true ]; then
    echo "Pushing image to registry..."
    docker push "${FULL_IMAGE_NAME}"

    # Also push latest tag for release versions
    if [[ "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
        LATEST_TAG="latest-${IMAGE_TAG_SUFFIX}"
        if [ -n "$REGISTRY_PREFIX" ]; then
            LATEST_NAME="${REGISTRY_PREFIX}/${IMAGE_NAME}:${LATEST_TAG}"
        else
            LATEST_NAME="${IMAGE_NAME}:${LATEST_TAG}"
        fi
        echo "Tagging and pushing: ${LATEST_NAME}"
        docker tag "${FULL_IMAGE_NAME}" "${LATEST_NAME}"
        docker push "${LATEST_NAME}"
    fi
fi

echo ""
echo "Done!"