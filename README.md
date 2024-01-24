### Run the following to build the docker containers and setup the docker network

```
docker build -t load_balancer ./loadbalancer
docker build -t server ./server
docker-compose up -d
```
