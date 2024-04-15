# Use an official Python runtime as a parent image
FROM python:3.8-slim-buster

# install requirements
RUN pip install fastapi
RUN pip install httpx
RUN pip install mysql-connector-python  --break-system-packages
RUN pip install uvicorn
# Make port 8080 available to the world outside this container
EXPOSE 8080

WORKDIR /app
ADD . /app

# run main.py when the container launches
CMD ["python", "shard_manager.py"]