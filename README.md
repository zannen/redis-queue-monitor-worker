# Redis queue monitor and worker

This docker image acts as:

- a redis queue monitor, providing Prometheus-compatible statistics
- a redis queue worker, processing jobs from a queue

# Running a queue monitor

## docker
```shell
docker run --rm -ti \
	-e REDIS_HOST=redis-server \
	-e REDIS_PORT=36379 \
	oreandawe/redis-queue-monitor-worker:latest
```

## kubernetes

See [`k8s-rq-monitor.yaml`](/k8s-rq-monitor.yaml).

# Running a queue worker

## docker
```shell
queue_name=high
worker=app.MyWorker  # see app/__init__.py for class definition
redis_server=redis://redis-server:36379
docker run --rm -ti \
	--entrypoint rq \
	oreandawe/redis-queue-monitor-worker:latest \
		worker \
		"$queue_name" \
		-w "$worker" \
		--url "$redis_server"
```

## kubernetes

See [`k8s-rq-worker-high.yaml`](/k8s-rq-worker-high.yaml).

# Licence

This is free and unencumbered software released into the public domain. See [`LICENSE`](/LICENSE) for details.
