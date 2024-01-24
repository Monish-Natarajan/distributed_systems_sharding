# Use latest Python runtime as base image
FROM python:latest

# Set the working directory in the container to /app
WORKDIR /app
# Install docker cli to spawn other containers
RUN apt-get update
RUN apt-get -y install sudo

RUN apt-get -y install apt-transport-https ca-certificates curl gnupg2 software-properties-common
RUN curl -fsSL https://download.docker.com/linux/debian/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg
RUN chmod a+r /etc/apt/keyrings/docker.gpg
RUN echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/debian \
  $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
  tee /etc/apt/sources.list.d/docker.list > /dev/null
RUN apt-get update
RUN apt-get -y install docker-ce-cli

# Install necessary packages
RUN pip install quart
RUN pip install httpx
EXPOSE 5000

# C++
RUN apt-get install -y cmake
# install Boost::Python library
RUN apt-get install -y libboost-python-dev

WORKDIR /app
COPY . .
# Build the C++ code
WORKDIR /app/build
RUN cmake ..
RUN cmake --build .
WORKDIR /app


# Run lb.py when the container launches
CMD ["python", "lb.py"]