# Use an official Python runtime as a parent image
FROM python:3.8-slim-buster

# Set the working directory in the container to /app
WORKDIR /app

# copy the current directory contents into the container at /app
COPY . /app

# Install docker cli to spawn other containers
RUN apt-get update
RUN apt-get -y install sudo

RUN apt-get -y install apt-transport-https ca-certificates curl gnupg2 software-properties-common
RUN curl -fsSL https://download.docker.com/linux/debian/gpg | apt-key add -
RUN add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/debian $(lsb_release -cs) stable"
RUN apt-get update
RUN apt-get -y install docker-ce-cli

# ENV USER=theuser
# RUN adduser --home /home/$USER --disabled-password --gecos GECOS $USER \
#   && echo "$USER ALL=(ALL) NOPASSWD: ALL" > /etc/sudoers.d/$USER \
#   && chmod 0440 /etc/sudoers.d/$USER \
#   && groupadd docker \
#   && usermod -aG docker $USER \
#   && chsh -s /bin/zsh $USER
# USER $USER

# ENV HOME=/home/$USER

# Run lb.py when the container launches
CMD ["python", "spawner.py"]