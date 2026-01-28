# DuckDB Multi-Component Build System

This directory contains the complete build infrastructure for multiple DuckDB components:

- **DuckLake Extension** (parent repository - custom extension)
- **DuckDB Postgres Scanner** (postgres_scanner extension)  
- **DuckDB Java JDBC** (with custom DuckDB branch: feature/branching)

## 🚀 Quick Start

### Jenkins Pipeline (Recommended)

1. **Configure Jenkins Environment Variables:**
   ```bash
   S3_BUCKET=your-artifacts-bucket
   S3_PREFIX=duckdb-artifacts  
   AWS_REGION=us-east-1
   ```

2. **Create Pipeline Job:**
   - Use `ci/Jenkinsfile.complete` as the pipeline script
   - Configure SCM to point to this repository

3. **Run Build with Parameters:**
   - `BUILD_TARGET`: `all`, `ducklake`, `postgres`, `java`
   - `PARALLEL_BUILD`: `true` (faster) or `false` (sequential)
   - `S3_BUCKET_OVERRIDE`: Optional bucket override
   - `S3_PREFIX_OVERRIDE`: Optional prefix override

### Shell Script (Local/Manual)

```bash
# Build all components (run from project root)
./ci/build-all.sh all my-bucket duckdb-artifacts

# Build specific components
./ci/build-all.sh ducklake
./ci/build-all.sh postgres  
./ci/build-all.sh java

# With environment variables
export S3_BUCKET="my-bucket"
export S3_PREFIX="custom-path"
./ci/build-all.sh all
```

### Docker Compose (Development/Testing)

```bash
# Navigate to ci directory
cd ci

# Build all components locally (no S3 upload)
docker-compose up build-all

# Build individual components for testing
docker-compose up ducklake
docker-compose up postgres
docker-compose up java

# Test specific component
docker-compose up test-ducklake
```

## 📦 Artifacts & S3 Structure

### Generated Artifacts
```
artifacts/ directory (created in project root):
├── ducklake-{version}.duckdb_extension
├── postgres_scanner-{version}.duckdb_extension
└── duckdb_jdbc-{version}.jar
```

### S3 Structure
```
s3://your-bucket/duckdb-artifacts/
├── extensions/
│   ├── ducklake-{version}.duckdb_extension
│   ├── ducklake-latest.duckdb_extension
│   ├── postgres_scanner-{version}.duckdb_extension
│   └── postgres_scanner-latest.duckdb_extension
└── java/
    ├── duckdb_jdbc-{version}.jar
    └── duckdb_jdbc-latest.jar
```

### Artifact Naming Convention
- **Versioned**: `{component}-{YYYYMMDD-HHMMSS}-{git-hash}.{ext}`
- **Latest**: `{component}-latest.{ext}`

## 🏗️ Build Process Details

### 1. DuckLake Extension (Parent Repository)
```bash
# Uses ci/Dockerfile.ducklake
docker build -f ci/Dockerfile.ducklake -t ducklake-build .
# Extracts: /app/build/release/extension/ducklake/ducklake.duckdb_extension
```

### 2. DuckDB Postgres Scanner
```bash
# Clones: https://github.com/duckdb/duckdb-postgres.git
# Uses: ci/Dockerfile.postgres
# Extracts: /app/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension
```

### 3. DuckDB Java JDBC
```bash
# Clones: https://github.com/duckdb/duckdb-java.git
# Clones: https://github.com/peernova/duckdb.git (feature/branching)
# Uses: ci/Dockerfile.java
# Process: python3 vendor.py --duckdb ../duckdb && make release
# Extracts: /workspace/duckdb-java/build/release/duckdb_jdbc.jar
```

## 🔧 Configuration

### Environment Variables
| Variable | Default | Description |
|----------|---------|-------------|
| `S3_BUCKET` | `your-bucket-name` | S3 bucket for artifacts |
| `S3_PREFIX` | `duckdb-artifacts` | S3 path prefix |
| `AWS_REGION` | `us-east-1` | AWS region |
| `GEN` | `ninja` | Build generator |

### Repository Dependencies
The build system automatically clones these repositories:
- **DuckDB Postgres**: `https://github.com/duckdb/duckdb-postgres.git`
- **DuckDB Java**: `https://github.com/duckdb/duckdb-java.git`  
- **DuckDB Core**: `https://github.com/peernova/duckdb.git` (branch: `feature/branching`)

## 📋 File Structure

```
ducklake/                           # Parent repository
├── ci/                             # Build infrastructure
│   ├── Dockerfile.ducklake         # DuckLake extension build
│   ├── Dockerfile.postgres         # Postgres scanner build  
│   ├── Dockerfile.java             # Java JDBC build
│   ├── Jenkinsfile.complete        # Complete Jenkins pipeline
│   ├── build-all.sh                # Shell script for all builds
│   ├── docker-compose.yml          # Local development setup
│   └── BUILD.md                    # This documentation
├── build-repos/                    # Auto-created for cloned repos
│   ├── duckdb-postgres/           # (cloned during build)
│   ├── duckdb-java/               # (cloned during build)
│   └── duckdb/                    # (cloned during build)
└── artifacts/                      # Build outputs (auto-created)
    ├── ducklake-{version}.duckdb_extension
    ├── postgres_scanner-{version}.duckdb_extension
    └── duckdb_jdbc-{version}.jar
```

## 🚀 Jenkins Pipeline Features

### Parallel Builds
- **Enabled**: All components build simultaneously (faster)
- **Disabled**: Sequential builds (lower resource usage)

### Build Parameters
- **Flexible targeting**: Choose specific components or build all
- **S3 configuration**: Override bucket/prefix per build
- **Build reports**: Comprehensive summaries with S3 URLs

### Error Handling
- **Robust cleanup**: Docker images and temporary files
- **Failure recovery**: Individual component failures don't stop others
- **Detailed logging**: Complete build process visibility

## 🔍 Troubleshooting

### Common Issues

**Docker Build Failures:**
- Check available disk space and memory
- Verify Docker daemon is running
- Review build logs for specific errors

**S3 Upload Failures:**
- Verify AWS credentials are configured
- Check bucket permissions (s3:PutObject, s3:PutObjectAcl)
- Ensure bucket exists and region is correct

**Git Clone Failures:**
- Verify network connectivity
- Check repository access permissions
- Ensure git is installed in build environment

### Build Logs
- **Jenkins**: Full logs available in build console
- **Docker**: Use `docker logs <container-name>`
- **Local**: Script outputs detailed progress information

## 🤝 Development Workflow

### Adding New Components
1. Create new Dockerfile in `ci/` directory (e.g., `ci/Dockerfile.newcomponent`)
2. Add build function to `ci/build-all.sh`
3. Update Jenkins pipeline with new build stage
4. Add to `ci/docker-compose.yml` for local testing
5. Update this documentation

### Testing Changes
```bash
# Test individual component locally (from ci directory)
cd ci && docker-compose up test-ducklake

# Test full pipeline locally (from project root)
./ci/build-all.sh all

# Test without S3 upload (from ci directory)
cd ci && docker-compose up build-all
```

### Best Practices
- **Test locally** before pushing to Jenkins
- **Use parallel builds** for faster CI/CD
- **Monitor S3 costs** - clean up old artifacts periodically
- **Version control** build infrastructure changes
- **Document** any new environment variables or dependencies

## 📊 Monitoring & Maintenance

### S3 Artifact Management
```bash
# List recent artifacts
aws s3 ls s3://your-bucket/duckdb-artifacts/ --recursive

# Clean up old artifacts (keep last 10 versions)
aws s3api list-objects-v2 --bucket your-bucket --prefix duckdb-artifacts/ \
  --query 'sort_by(Contents,&LastModified)[:-10].Key' --output text | \
  xargs -I {} aws s3 rm s3://your-bucket/{}
```

### Build Performance
- **Parallel builds**: ~3x faster than sequential
- **Docker layer caching**: Significant speedup on repeated builds
- **Resource usage**: Monitor CPU/memory during parallel builds

This build system provides a complete, production-ready solution for building and distributing all DuckDB components from a single repository with organized CI infrastructure.