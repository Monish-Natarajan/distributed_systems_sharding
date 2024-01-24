### Run the following to build the docker containers and setup the docker network

```
docker build -t myloadbal ./loadbalancer
docker build -t myserver ./server
docker-compose up -d
```
