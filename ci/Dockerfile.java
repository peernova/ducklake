# DuckDB Java JDBC Build with Custom DuckDB Branch
FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=UTC

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    ninja-build \
    git \
    python3 \
    python3-pip \
    curl \
    wget \
    pkg-config \
    libssl-dev \
    openjdk-21-jdk \
    maven \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64

WORKDIR /workspace

# Copy cloned repositories from build-repos directory
COPY build-repos/duckdb-java/ ./duckdb-java/
COPY build-repos/duckdb/ ./duckdb/

# Build duckdb-java with custom duckdb
WORKDIR /workspace/duckdb-java

# Set environment variable for ninja generator
ENV GEN=ninja

# Run vendor script and build
RUN python3 vendor.py --duckdb ../duckdb && \
    make release

CMD ["bash"]