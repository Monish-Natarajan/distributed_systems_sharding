setup:
	pip install -r requirements.txt
	-@docker network create mynet
	@docker build -t server ./server

run:
	docker compose up -d
stop:
	@docker compose down --rmi local
	@docker ps -a -q --filter ancestor=server | xargs -r docker rm --force
	@docker rmi server --force
analysis:
	python3 analysis.py