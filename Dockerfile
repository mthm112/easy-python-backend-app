# Use the official Python image from Docker Hub
FROM python:3.8-slim

# Set work directory in the container
WORKDIR /app

# Copy project files into the docker image
COPY . .

# Install project dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Set environment variable FLASK_APP
ENV FLASK_APP=app.py

# Set a default port and allow to override it
ENV PORT=5000

# Expose the default port or the one set in the environment
EXPOSE $PORT

# The command to run your application
CMD flask run --host=0.0.0.0 --port=${PORT:-5000}