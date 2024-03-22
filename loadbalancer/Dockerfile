# Use latest Python runtime as base image
FROM ubuntu:latest

# Set the working directory in the container to /app
WORKDIR /app
# Install docker cli to spawn other containers
RUN apt-get update
RUN apt-get -y install sudo
RUN apt-get -y install python3-all
RUN apt-get -y install apt-transport-https ca-certificates curl gnupg2 software-properties-common
RUN install -m 0755 -d /etc/apt/keyrings
RUN curl -fsSL https://download.docker.com/linux/debian/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg
RUN chmod a+r /etc/apt/keyrings/docker.gpg
RUN echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
  $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
  tee /etc/apt/sources.list.d/docker.list > /dev/null
RUN apt-get update
RUN apt-get -y install docker-ce-cli

# Install necessary packages
ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get -y install python3-pip
RUN apt-get -y install python3-fastapi python3-uvicorn python3-httpx
RUN apt-get -y install python3-mysql.connector
#RUN pip install fastapi uvicorn
#RUN pip install httpx
EXPOSE 5001

# C++
RUN apt-get install -y cmake
# install Boost::Python library
RUN apt-get install -y libboost-python-dev
RUN apt-get install -y gdb
RUN apt-get install -y libpython3-dbg

WORKDIR /app
COPY . .
# Build the C++ code
WORKDIR /app/build
# Do DEBUG build
RUN cmake -DCMAKE_BUILD_TYPE=Release ..
RUN cmake --build .
WORKDIR /app

# Launch a mysql server
CMD ["docker", "run", "--name", "mysql_server", "-e", "MYSQL_ROOT_PASSWORD=testing", "-e", "MYSQL_DATABASE=load_balancer_database", "-e", "MYSQL_USER=ravanan", "-e", "MYSQL_PASSWORD=testing", "-p", "3306:3306", "-d", "mysql:latest"]
# Run lb.py when the container launches
CMD ["python", "lb_fast.py"]
#CMD python -m cProfile -o profiling.txt lb.py && less profiling.txt