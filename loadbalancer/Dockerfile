# Use an official Python runtime as a parent image
FROM python:3.8-slim-buster

# Set the working directory in the container to /app
WORKDIR /app

# copy the current directory contents into the container at /app
COPY . /app

# Install necessary packages
RUN pip install quart
RUN pip install httpx
EXPOSE 5000

# Run lb.py when the container launches
CMD ["python", "lb.py"]