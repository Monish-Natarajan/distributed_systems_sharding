#Dockerfile
 #----------------------
FROM mysql:8.0-debian

COPY deploy.sh /always-initdb.d/
#here the flask app deploy script is copied
WORKDIR /app
COPY . /app

RUN apt-get update
RUN apt-get install -y python3 python3-pip python3-venv python3-fastapi python3-uvicorn
 
ENV MYSQL_ROOT_PASSWORD="testing"
ENV MYSQL_DATABASE="distributed_systems"

# create and use a virtual env
# Install any needed packages needed
RUN pip install mysql.connector --break-system-packages
EXPOSE 8080

ENTRYPOINT ["/app/custom-entry.sh"]
CMD ["mysqld"]