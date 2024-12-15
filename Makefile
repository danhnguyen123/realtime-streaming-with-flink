
PLATFORM ?= linux/amd64
IMAGE_NAME ?= flink-image

# COLORS
GREEN  := $(shell tput -Txterm setaf 2)
YELLOW := $(shell tput -Txterm setaf 3)
WHITE  := $(shell tput -Txterm setaf 7)
RESET  := $(shell tput -Txterm sgr0)


TARGET_MAX_CHAR_NUM=20

## Show help with `make help`
help:
	@echo ''
	@echo 'Usage:'
	@echo '  ${YELLOW}make${RESET} ${GREEN}<target>${RESET}'
	@echo ''
	@echo 'Targets:'
	@awk '/^[a-zA-Z\-\_0-9]+:/ { \
		helpMessage = match(lastLine, /^## (.*)/); \
		if (helpMessage) { \
			helpCommand = substr($$1, 0, index($$1, ":")-1); \
			helpMessage = substr(lastLine, RSTART + 3, RLENGTH); \
			printf "  ${YELLOW}%-$(TARGET_MAX_CHAR_NUM)s${RESET} ${GREEN}%s${RESET}\n", helpCommand, helpMessage; \
		} \
	} \
	{ lastLine = $$0 }' $(MAKEFILE_LIST)

.PHONY: build
## Builds the Flink base image with pyFlink and connectors installed
build:
	docker build -t ${IMAGE_NAME} .

.PHONY: up
## Builds the base Docker image and starts Flink cluster
up:
	docker compose up -d

.PHONY: down
## Shuts down the Flink cluster
down:
	docker compose down

.PHONY: job
## Submit the Flink job
transaction_job:
	docker compose exec jobmanager ./bin/flink run -py /opt/src/transaction_job.py --pyFiles /opt/src -d

aggregation_job:
	docker compose exec jobmanager ./bin/flink run -py /opt/src/aggregated_job.py --pyFiles /opt/src -d

.PHONY: stop
## Stops all services in Docker compose
stop:
	docker compose stop

.PHONY: start
## Starts all services in Docker compose
start:
	docker compose start

.PHONY: clean
## Stops and removes the Docker container as well as images with tag `<none>`
clean:
	docker compose stop
	docker ps -a --format '{{.Names}}' | grep "^${CONTAINER_PREFIX}" | xargs -I {} docker rm {}
	docker images | grep "<none>" | awk '{print $3}' | xargs -r docker rmi
	# Uncomment line `docker rmi` if you want to remove the Docker image from this set up too
	# docker rmi ${IMAGE_NAME}


list_topic:
	docker exec -it broker kafka-topics --list --bootstrap-server broker:29092

consume_message:
	docker exec -it broker kafka-console-consumer --topic financial_transactions --bootstrap-server broker:29092 --from-beginning

produce_message:
	python3 ./SalesTransactionGenerator/main.py
