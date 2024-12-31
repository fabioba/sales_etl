# SERVER

## Getting Started
Build the Docker Image Run the following command to build the image:

```yaml
docker build -t sales-data-server .
```

Run the Docker Container Start the container with the following command:
```yaml
docker run -p 5000:5000 sales-data-server
```

## Logic
What Happens When You Access the App:
1. A request to localhost:5001 on the host machine is received by Docker.
2. Docker forwards the request to port 5000 inside the container.
3. The Flask app listens on 5000 and handles the request.