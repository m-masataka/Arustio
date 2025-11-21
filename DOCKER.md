# Docker Environment Guide

This guide explains how to use Arustio with Docker and MinIO.

## Quick Start

```bash
docker-compose up -d
sleep 10
docker-compose exec arustio arustio-server --example docker
```

## Architecture

```
┌─────────────────────────────────────┐
│         Arustio Server              │
│    (Rust Application in Docker)     │
└────────────┬────────────────────────┘
             │
             │ S3 API
             ↓
┌─────────────────────────────────────┐
│           MinIO                      │
│  (S3-Compatible Object Storage)     │
│                                      │
│  Buckets:                            │
│  - test-bucket                       │
│  - data-bucket                       │
│  - temp-bucket                       │
└─────────────────────────────────────┘
```

## Services

### MinIO
- **API Port**: 9000
- **Console Port**: 9001
- **Console URL**: http://localhost:9001
- **Credentials**: minioadmin / minioadmin

### Arustio Server
- Pre-configured to connect to MinIO
- Automatically mounts three MinIO buckets

## Usage Examples

### 1. Basic Docker Test

```bash
# Start environment
docker-compose up -d

# Wait for initialization
sleep 10

# Run the docker example
docker-compose exec arustio arustio-server --example docker
```

This will:
- Mount 3 different buckets to different paths
- Create directories and files in each bucket
- Read files from different buckets
- Perform cross-bucket operations
- List directory contents

### 2. Interactive Shell

```bash
# Enter the container
docker-compose exec arustio /bin/bash

# Inside the container, you can run:
arustio-server --example basic
arustio-server --example mount
arustio-server --example docker
```

### 3. View Data in MinIO Console

1. Open http://localhost:9001 in your browser
2. Login with: minioadmin / minioadmin
3. Browse the buckets:
   - `test-bucket` - Contains documents
   - `data-bucket` - Contains datasets
   - `temp-bucket` - Contains cache files

### 4. Custom Commands

```bash
# View logs
docker-compose logs -f arustio

# View MinIO logs
docker-compose logs -f minio

# Restart services
docker-compose restart

# Check service status
docker-compose ps
```

## Mount Points

The Docker example creates the following mount structure:

```
/mnt/
├── s3-test/     → test-bucket (MinIO)
│   └── documents/
│       ├── readme.txt
│       └── config.json
├── s3-data/     → data-bucket (MinIO)
│   ├── datasets/
│   │   └── sample.csv
│   └── backup/
│       └── session-backup.txt
└── s3-temp/     → temp-bucket (MinIO)
    └── cache/
        └── session.txt
```

## Environment Variables

The following environment variables are configured in `docker-compose.yml`:

```yaml
S3_BUCKET: test-bucket
S3_REGION: us-east-1
S3_ENDPOINT: http://minio:9000
AWS_ACCESS_KEY_ID: minioadmin
AWS_SECRET_ACCESS_KEY: minioadmin
```

You can modify these in the docker-compose.yml file.

## Troubleshooting

### Services not starting

```bash
# Check logs
docker-compose logs

# Restart everything
docker-compose down
docker-compose up -d
```

### Connection errors

Make sure MinIO is fully initialized:
```bash
# Check MinIO health
curl http://localhost:9000/minio/health/live

# Wait a bit longer
sleep 15
docker-compose exec arustio arustio-server --example docker
```

### Reset everything

```bash
# Stop and remove all data
docker-compose down -v

# Start fresh
docker-compose up -d
sleep 10
docker-compose exec arustio arustio-server --example docker
```

## Development

### Building the Docker Image

```bash
# Build the image
docker-compose build

# Or rebuild without cache
docker-compose build --no-cache
```

### Running Tests

```bash
# Run all examples
docker-compose exec arustio arustio-server --example basic
docker-compose exec arustio arustio-server --example mount
docker-compose exec arustio arustio-server --example docker
```

### Modifying the Code

After making changes:

```bash
# Rebuild and restart
docker-compose down
docker-compose build
docker-compose up -d
```

## Cleanup

```bash
# Stop services (keeps data)
docker-compose down

# Stop and remove all data
docker-compose down -v

# Remove images too
docker-compose down -v --rmi all
```

## Using with Real AWS S3

To use real AWS S3 instead of MinIO, modify the environment variables:

```yaml
environment:
  S3_BUCKET: your-actual-bucket
  S3_REGION: us-west-2
  # Remove S3_ENDPOINT to use real AWS
  # S3_ENDPOINT:
  AWS_ACCESS_KEY_ID: your-access-key
  AWS_SECRET_ACCESS_KEY: your-secret-key
```

## Next Steps

- Try mounting additional buckets
- Experiment with different storage backends (GCS, Azure)
- Implement custom file operations
- Test with larger files and datasets
