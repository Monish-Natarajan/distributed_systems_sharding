### Run the following to build the docker containers and setup the docker network

```
docker build -t myloadbal ./loadbalancer
docker build -t myserver ./server
docker-compose up -d
```

### TODO
- [ ] consistent hashing
- [ ] maintaining multiple copies of each server
- [ ] make lb as a privileged container
- [ ] add(), rem()
- [ ] implement heartbeat requests
- [ ] spawn new server in case of failure
- [ ] error checking
- [ ] Simulation and analysis
