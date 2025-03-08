IMAGE_NAME := polygon-pos-indexer
CONTAINER_NAME := polygon-post-indexer-container

build:
	@echo "Building Docker image..."
	docker build -t $(IMAGE_NAME) .
	@echo "Docker image built successfully: $(IMAGE_NAME)"

run: build
	@echo "Running Docker container in application mode..."
	docker run --rm --name $(CONTAINER_NAME) -v "$(PWD):/app" $(IMAGE_NAME)

shell: build
	@echo "Ensuring no conflicting shell container exists..."
	-docker rm -f "$(CONTAINER_NAME)-shell" 2>/dev/null || true
	@echo "Starting interactive shell in Docker container..."
	docker run -it --name "$(CONTAINER_NAME)-shell" -v "$(PWD):/app" $(IMAGE_NAME) /bin/bash

test: build
	@echo "Running tests in Docker container..."
	docker run --rm -v "$(PWD):/app" $(IMAGE_NAME) python -m pytest tests/ -v

stop:
	@echo "Stopping Docker container..."
	-docker stop $(CONTAINER_NAME) 2>/dev/null || true
	@echo "Stopped application container (if running)."
	-docker stop "$(CONTAINER_NAME)-shell" 2>/dev/null || true
	@echo "Stopped interactive shell container (if running)."
	-docker rm "$(CONTAINER_NAME)-shell" 2>/dev/null || true
	@echo "Removed interactive shell container (if existed)."

.PHONY: build run shell stop test test-coverage
