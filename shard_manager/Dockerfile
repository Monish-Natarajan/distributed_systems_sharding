# Use an official Python runtime as a parent image
FROM python:3.8-slim-buster

WORKDIR /app
ADD . /app

# install requirements
RUN pip install fastapi
RUN pip install httpx

# Make port 80 available to the world outside this container
EXPOSE 8080

# run main.py when the container launches
CMD ["python", "main.py"]