# Use rcom/tensorflow:latest as the base image
FROM rocm/tensorflow:latest

# Set the working directory to /app
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install necessary system packages
#RUN apt-get update && apt-get install -y libcairo2-dev

# Create a virtual environment and activate it
RUN python3 -m venv venv
ENV PATH="/app/venv/bin:$PATH"

# Install your Python dependencies
#COPY requirements.txt .
#RUN pip install --no-cache-dir -r requirements.txt