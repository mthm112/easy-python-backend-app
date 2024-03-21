# We use the official Python image from Docker Hub
FROM python:3.8

# We create directory for our application
WORKDIR /usr/src/app

# We install dependencies
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# We copy the source code of our application into the Docker image
COPY . .

# We set the environment variable FLASK_APP
ENV FLASK_APP=app.py

# CMD defines the standard command that should be run when starting the container
CMD [ "python", "-m" , "flask", "run", "--host=0.0.0.0"]