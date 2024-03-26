DOCKER_GID := $(shell getent group docker | cut -d ':' -f 3)
setup:
	-@pip install -r requirements.txt
	-@docker network create mynet
	@docker build -t server ./server
	@docker compose build

run:
	@docker compose build
	DOCKER_GID=$(DOCKER_GID) docker compose up -d
stop:
	-@docker compose down --rmi local
	-@docker ps -a -q --filter ancestor=server | xargs -r docker rm --force
clean: stop
	-@docker rmi server --force
	-@docker network rm mynet
	
analysis:
	python3 analysis.py