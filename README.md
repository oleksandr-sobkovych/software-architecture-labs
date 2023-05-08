# [Lab 3: Microservices & Hazelcast](https://github.com/oleksandr-sobkovych/software-architecture-labs/tree/micro_hazelcast)
   Authors: [Oleksandr Sobkovych](https://github.com/oleksandr-sobkovych)
## Prerequisites

- docker compose
- curl or another http client

## Usage

Setup variables in the .env file:

```bash
MESSAGES_SERVICE_PORT=50051
LOGGING_SERVICE_PORT=50051
NGINX_LOGGING_SERVICE_PORT=4000
# Both inner and outer
FACADE_SERVICE_PORT=5000
RUST_LOG=info
```

Change nginx configuration accordingly:

```nginx
...
      server logging-service:50051;
...
      listen 4000 http2;
...
```

Build and run containers:

```bash
docker-compose up --build
```

Send GET and POST requests to the facade-service:

```bash
curl -X GET http://localhost:5000
curl -d "Hello" -X POST http://localhost:5000
```

Access Hazelcast Management Center at [http://localhost:8080/](http://localhost:8080/)
