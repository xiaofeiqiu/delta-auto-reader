# Delta Auto Reader Docker Makefile

# Image settings
IMAGE_NAME := delta-auto-reader
IMAGE_TAG := latest
CONTAINER_NAME := delta-reader-container

# Ports
JUPYTER_PORT := 8888
SPARK_UI_PORT := 4040

# Default target
.DEFAULT_GOAL := help

# Build the Docker image
.PHONY: build
build:
	@echo "Building Docker image: $(IMAGE_NAME):$(IMAGE_TAG)"
	docker build -t $(IMAGE_NAME):$(IMAGE_TAG) .

# Run Jupyter notebook in container
.PHONY: jupyter
jupyter: build setup-dirs clean-containers
	@echo "Starting Jupyter notebook server..."
	@echo "Access at: http://localhost:$(JUPYTER_PORT)"
	@echo "Press Ctrl+C to stop the server"
	docker run --rm \
		--name $(CONTAINER_NAME)-jupyter \
		-p $(JUPYTER_PORT):8888 \
		-p $(SPARK_UI_PORT):4040 \
		-v $(PWD)/data:/workspace/data \
		-v $(PWD)/notebooks:/workspace/notebooks \
		-v $(PWD)/feature_store_sdk:/workspace/feature_store_sdk \
		$(IMAGE_NAME):$(IMAGE_TAG) jupyter

# Run PySpark shell in container
.PHONY: pyspark
pyspark: build
	@echo "Starting PySpark shell..."
	docker run -it --rm \
		--name $(CONTAINER_NAME)-pyspark \
		-p $(SPARK_UI_PORT):4040 \
		-v $(PWD)/data:/workspace/data \
		$(IMAGE_NAME):$(IMAGE_TAG) pyspark

# Run Scala Spark shell in container
.PHONY: spark-shell
spark-shell: build
	@echo "Starting Spark shell (Scala)..."
	docker run -it --rm \
		--name $(CONTAINER_NAME)-spark \
		-p $(SPARK_UI_PORT):4040 \
		-v $(PWD)/data:/workspace/data \
		$(IMAGE_NAME):$(IMAGE_TAG) spark-shell

# Run bash shell in container
.PHONY: bash
bash: build
	@echo "Starting bash shell..."
	docker run -it --rm \
		--name $(CONTAINER_NAME)-bash \
		-v $(PWD)/data:/workspace/data \
		-v $(PWD)/notebooks:/workspace/notebooks \
		-v $(PWD)/feature_store_sdk:/workspace/feature_store_sdk \
		$(IMAGE_NAME):$(IMAGE_TAG) bash

# Run container with custom command
.PHONY: run
run: build
	@echo "Running custom command in container..."
	@echo "Usage: make run CMD='your-command-here'"
	docker run -it --rm \
		--name $(CONTAINER_NAME)-custom \
		-v $(PWD)/data:/workspace/data \
		-v $(PWD)/notebooks:/workspace/notebooks \
		-v $(PWD)/feature_store_sdk:/workspace/feature_store_sdk \
		$(IMAGE_NAME):$(IMAGE_TAG) $(CMD)

# Create necessary directories
.PHONY: setup-dirs
setup-dirs:
	@echo "Creating necessary directories..."
	mkdir -p data notebooks

# Clean up only Docker containers (keeps image)
.PHONY: clean-containers
clean-containers:
	@echo "Cleaning up existing containers..."
	-docker stop $(CONTAINER_NAME)-jupyter $(CONTAINER_NAME)-pyspark $(CONTAINER_NAME)-spark $(CONTAINER_NAME)-bash $(CONTAINER_NAME)-custom 2>/dev/null
	-docker rm $(CONTAINER_NAME)-jupyter $(CONTAINER_NAME)-pyspark $(CONTAINER_NAME)-spark $(CONTAINER_NAME)-bash $(CONTAINER_NAME)-custom 2>/dev/null

# Clean up Docker images and containers
.PHONY: clean
clean:
	@echo "Cleaning up Docker resources..."
	-docker stop $(CONTAINER_NAME)-jupyter $(CONTAINER_NAME)-pyspark $(CONTAINER_NAME)-spark $(CONTAINER_NAME)-bash $(CONTAINER_NAME)-custom 2>/dev/null
	-docker rm $(CONTAINER_NAME)-jupyter $(CONTAINER_NAME)-pyspark $(CONTAINER_NAME)-spark $(CONTAINER_NAME)-bash $(CONTAINER_NAME)-custom 2>/dev/null
	-docker rmi $(IMAGE_NAME):$(IMAGE_TAG) 2>/dev/null

# Remove all related Docker resources (including volumes)
.PHONY: clean-all
clean-all: clean
	@echo "Removing all related Docker resources..."
	-docker system prune -f
	-docker volume prune -f

# Show Docker logs for running container
.PHONY: logs
logs:
	@echo "Available containers:"
	@docker ps --filter "name=$(CONTAINER_NAME)" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
	@echo ""
	@echo "Usage: make logs CONTAINER=container-suffix (e.g., jupyter, pyspark, etc.)"
	@if [ -n "$(CONTAINER)" ]; then \
		docker logs -f $(CONTAINER_NAME)-$(CONTAINER); \
	fi

# Test the Docker image by running a simple Delta Lake test
.PHONY: test
test: build
	@echo "Testing Delta Lake functionality..."
	docker run --rm \
		-v $(PWD)/data:/workspace/data \
		$(IMAGE_NAME):$(IMAGE_TAG) python3 -c "\
import pyspark; \
from pyspark.sql import SparkSession; \
from delta import *; \
builder = pyspark.sql.SparkSession.builder.appName('DeltaTest') \
    .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension') \
    .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog'); \
spark = configure_spark_with_delta_pip(builder).getOrCreate(); \
print('✅ Spark version:', spark.version); \
print('✅ Delta Lake configured successfully'); \
import pandas as pd; \
import polars as pl; \
print('✅ Pandas version:', pd.__version__); \
print('✅ Polars version:', pl.__version__); \
spark.stop(); \
print('✅ All components working correctly!')"

# Run comprehensive E2E test with Feature Store SDK demo
.PHONY: test-e2e
test-e2e: build setup-dirs clean-containers
	@echo "Running comprehensive E2E test with Feature Store SDK demo..."
	@echo "This will test all major functionality including:"
	@echo "  - FeatureSourceProjection (renamed from Projection)"
	@echo "  - feature_group parameter (renamed from source)"
	@echo "  - where parameter (renamed from filters)"
	@echo "  - transforms parameter (renamed from transform)"
	@echo "  - Multi-table joins, filtering, and output formats"
	@echo ""
	docker run --rm \
		-v $(PWD)/data:/workspace/data \
		-v $(PWD)/notebooks:/workspace/notebooks \
		-v $(PWD)/feature_store_sdk:/workspace/feature_store_sdk \
		$(IMAGE_NAME):$(IMAGE_TAG) python /workspace/notebooks/feature_store_sdk_demo.py
	@echo ""
	@echo "🎉 E2E test completed successfully!"
	@echo "✅ All feature store functionality verified"
	@echo "✅ All parameter renames working correctly"
	@echo "✅ All output formats (Spark, Pandas, Polars) tested"
	@echo "✅ Filter functionality with tuple format tested"

# Show help
.PHONY: help
help:
	@echo "Delta Auto Reader Docker Commands:"
	@echo ""
	@echo "Build and Run:"
	@echo "  make build        - Build the Docker image"
	@echo "  make jupyter      - Run Jupyter notebook server (http://localhost:8888)"
	@echo "  make pyspark      - Run PySpark interactive shell"
	@echo "  make spark-shell  - Run Scala Spark shell"
	@echo "  make bash         - Run bash shell in container"
	@echo "  make run CMD='...'- Run custom command in container"
	@echo ""
	@echo "Setup and Maintenance:"
	@echo "  make setup-dirs   - Create data and notebooks directories"
	@echo "  make test         - Test Delta Lake functionality"
	@echo "  make test-e2e     - Run comprehensive E2E test with Feature Store SDK demo"
	@echo "  make logs CONTAINER=suffix - Show logs for running container"
	@echo ""
	@echo "Cleanup:"
	@echo "  make clean        - Remove Docker image and containers"
	@echo "  make clean-all    - Remove all Docker resources and volumes"
	@echo ""
	@echo "Ports:"
	@echo "  Jupyter: http://localhost:$(JUPYTER_PORT)"
	@echo "  Spark UI: http://localhost:$(SPARK_UI_PORT)"
	@echo ""
	@echo "Volumes mounted:"
	@echo "  ./data -> /workspace/data"
	@echo "  ./notebooks -> /workspace/notebooks"