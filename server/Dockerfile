# Use an official Python runtime as a parent image
FROM python:latest

# Set the working directory in the container to /app
WORKDIR /app

# Add the current directory contents into the container at /app
COPY . /app

# Install any needed packages specified in requirements.txt
RUN pip install quart
# Run server.py when the container launches
CMD ["python", "server.py"]