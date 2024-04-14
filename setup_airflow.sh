#!/bin/bash

# Setup script for Airflow-based data engineering pipeline
# This script sets up the environment and starts Airflow

set -e

echo " Setting up Airflow-based Data Engineering Pipeline..."

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    echo " Docker is not installed. Please install Docker first."
    exit 1
fi

# Check if Docker Compose is installed
if ! command -v docker-compose &> /dev/null; then
    echo " Docker Compose is not installed. Please install Docker Compose first."
    exit 1
fi

# Create necessary directories
echo " Creating necessary directories..."
mkdir -p logs plugins data landing bronze silver

# Set proper permissions
echo " Setting permissions..."
chmod -R 755 logs plugins data landing bronze silver

# Check if .env file exists
if [ ! -f .env ]; then
    echo "  .env file not found. Creating default .env file..."
    echo "Please update the GITHUB_TOKEN in .env file with your actual GitHub token."
fi

# Build Docker images
echo " Building Docker images..."
docker-compose build

# Initialize Airflow database
echo "  Initializing Airflow database..."
docker-compose up airflow-init

# Start Airflow services
echo " Starting Airflow services..."
docker-compose up -d

# Wait for services to be ready
echo "⏳ Waiting for services to be ready..."
sleep 30

# Check if services are running
echo " Checking service status..."
docker-compose ps

echo ""
echo " Setup completed successfully!"
echo ""
echo " Airflow Web UI: http://localhost:8080"
echo "   Username: admin"
echo "   Password: admin"
echo ""
echo " Available DAGs:"
echo "   - github_data_pipeline: Main data engineering pipeline"
echo ""
echo "  Useful commands:"
echo "   - View logs: docker-compose logs -f"
echo "   - Stop services: docker-compose down"
echo "   - Restart services: docker-compose restart"
echo ""
echo " Next steps:"
echo "   1. Update GITHUB_TOKEN in .env file"
echo "   2. Access Airflow Web UI at http://localhost:8080"
echo "   3. Enable and run the 'github_data_pipeline' DAG"
echo ""
