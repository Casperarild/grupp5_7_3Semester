FROM python:3.11-slim

WORKDIR /app

# Copy subscriber code into container
COPY . /app

# Install dependencies
RUN pip install --no-cache-dir paho-mqtt psycopg2-binary

# Run the subscriber
CMD ["python", "subscriber.py"]
