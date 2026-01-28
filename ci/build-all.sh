#!/bin/bash

# Complete build script for DuckDB components from DuckLake repository
# Usage: ./ci/build-all.sh [component] [s3-bucket] [s3-prefix]
# Components: all, ducklake, postgres, java

set -e  # Exit on any error

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Configuration
COMPONENT="${1:-all}"
S3_BUCKET="${2:-${S3_BUCKET:-your-bucket-name}}"
S3_PREFIX="${3:-${S3_PREFIX:-duckdb-artifacts}}"
AWS_REGION="${AWS_REGION:-us-east-1}"

# Build info
BUILD_VERSION="$(date +%Y%m%d-%H%M%S)-$(git rev-parse --short HEAD 2>/dev/null || echo 'unknown')"
GIT_BRANCH="$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo 'unknown')"

echo "🚀 Building DuckDB components from DuckLake repository..."
echo "📦 Component: ${COMPONENT}"
echo "🏷️  Version: ${BUILD_VERSION}"
echo "☁️  S3 Bucket: s3://${S3_BUCKET}/${S3_PREFIX}/"

# Change to project root
cd "$PROJECT_ROOT"

# Create artifacts directory
mkdir -p artifacts

# Clone additional repositories if needed
setup_repos() {
    if [[ "$COMPONENT" == "all" || "$COMPONENT" == "postgres" || "$COMPONENT" == "java" ]]; then
        echo "📥 Cloning additional repositories..."
        rm -rf build-repos
        mkdir -p build-repos
        cd build-repos
        
        if [[ "$COMPONENT" == "all" || "$COMPONENT" == "postgres" ]]; then
            git clone https://github.com/duckdb/duckdb-postgres.git
        fi
        
        if [[ "$COMPONENT" == "all" || "$COMPONENT" == "java" ]]; then
            git clone https://github.com/duckdb/duckdb-java.git
            git clone https://github.com/peernova/duckdb.git
            cd duckdb && git checkout feature/branching && git pull && cd ..
        fi
        
        cd ..
    fi
}

build_ducklake() {
    echo "🔨 Building DuckLake extension..."
    
    local docker_image="ducklake-build"
    local artifact_name="ducklake-${BUILD_VERSION}.duckdb_extension"
    
    # Build Docker image from current directory
    docker build -f ci/Dockerfile.ducklake -t ${docker_image}:${BUILD_VERSION} .
    
    # Extract extension
    local container_id=$(docker create ${docker_image}:${BUILD_VERSION})
    docker cp ${container_id}:/app/build/release/extension/ducklake/ducklake.duckdb_extension artifacts/${artifact_name}
    docker rm ${container_id}
    docker rmi ${docker_image}:${BUILD_VERSION}
    
    echo "✅ DuckLake built: artifacts/${artifact_name}"
}

build_postgres() {
    echo "🔨 Building DuckDB Postgres extension..."
    
    local docker_image="duckdb-postgres-build"
    local artifact_name="postgres_scanner-${BUILD_VERSION}.duckdb_extension"
    
    # Build Docker image from cloned postgres directory
    cd build-repos/duckdb-postgres
    docker build -f ../../ci/Dockerfile.postgres -t ${docker_image}:${BUILD_VERSION} .
    cd ../..
    
    # Extract extension
    local container_id=$(docker create ${docker_image}:${BUILD_VERSION})
    docker cp ${container_id}:/app/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension artifacts/${artifact_name}
    docker rm ${container_id}
    docker rmi ${docker_image}:${BUILD_VERSION}
    
    echo "✅ Postgres Scanner built: artifacts/${artifact_name}"
}

build_java() {
    echo "🔨 Building DuckDB Java JDBC..."
    
    local docker_image="duckdb-java-build"
    local artifact_name="duckdb_jdbc-${BUILD_VERSION}.jar"
    
    # Build Docker image with both java and duckdb repos
    docker build -f ci/Dockerfile.java -t ${docker_image}:${BUILD_VERSION} .
    
    # Extract JAR
    local container_id=$(docker create ${docker_image}:${BUILD_VERSION})
    docker cp ${container_id}:/workspace/duckdb-java/build/release/duckdb_jdbc.jar artifacts/${artifact_name}
    docker rm ${container_id}
    docker rmi ${docker_image}:${BUILD_VERSION}
    
    echo "✅ DuckDB JDBC built: artifacts/${artifact_name}"
}

upload_artifacts() {
    echo "☁️ Uploading artifacts to S3..."
    
    local uploaded_count=0
    
    for artifact in artifacts/*; do
        if [[ -f "$artifact" ]]; then
            local filename=$(basename "$artifact")
            local component=""
            local s3_path=""
            
            # Determine component and S3 path
            if [[ "$filename" == ducklake-* ]]; then
                component="ducklake"
                s3_path="extensions"
            elif [[ "$filename" == postgres_scanner-* ]]; then
                component="postgres_scanner"
                s3_path="extensions"
            elif [[ "$filename" == duckdb_jdbc-* ]]; then
                component="duckdb_jdbc"
                s3_path="java"
            else
                echo "⚠️ Unknown artifact type: $filename"
                continue
            fi
            
            # Upload versioned artifact
            aws s3 cp "$artifact" \
                "s3://${S3_BUCKET}/${S3_PREFIX}/${s3_path}/${filename}" \
                --region ${AWS_REGION} \
                --metadata "component=${component},build-timestamp=$(date -u +%Y-%m-%dT%H:%M:%SZ),git-branch=${GIT_BRANCH}" \
                --content-type "application/octet-stream"
            
            # Upload as latest
            local latest_name=""
            case "$component" in
                "ducklake")
                    latest_name="ducklake-latest.duckdb_extension"
                    ;;
                "postgres_scanner")
                    latest_name="postgres_scanner-latest.duckdb_extension"
                    ;;
                "duckdb_jdbc")
                    latest_name="duckdb_jdbc-latest.jar"
                    ;;
            esac
            
            aws s3 cp "$artifact" \
                "s3://${S3_BUCKET}/${S3_PREFIX}/${s3_path}/${latest_name}" \
                --region ${AWS_REGION} \
                --metadata "component=${component},build-timestamp=$(date -u +%Y-%m-%dT%H:%M:%SZ),git-branch=${GIT_BRANCH}" \
                --content-type "application/octet-stream"
            
            echo "📦 Uploaded: s3://${S3_BUCKET}/${S3_PREFIX}/${s3_path}/${filename}"
            echo "🔗 Latest: s3://${S3_BUCKET}/${S3_PREFIX}/${s3_path}/${latest_name}"
            
            ((uploaded_count++))
        fi
    done
    
    echo "✅ Uploaded ${uploaded_count} artifacts successfully!"
}

# Setup repositories
setup_repos

# Build components based on selection
case "$COMPONENT" in
    "all")
        build_ducklake
        build_postgres
        build_java
        ;;
    "ducklake")
        build_ducklake
        ;;
    "postgres")
        build_postgres
        ;;
    "java")
        build_java
        ;;
    *)
        echo "❌ Error: Unknown component: $COMPONENT"
        echo "Supported components: all, ducklake, postgres, java"
        exit 1
        ;;
esac

# Upload artifacts
upload_artifacts

# Clean up
rm -rf artifacts build-repos

echo "🎉 Build completed successfully!"
echo "📊 Summary:"
echo "   Component(s): ${COMPONENT}"
echo "   Version: ${BUILD_VERSION}"
echo "   S3 Location: s3://${S3_BUCKET}/${S3_PREFIX}/"