"""
Provide a Flask app
"""

import collections
import datetime
import logging
import os
import time
from typing import Any, Dict, List

import flask
import redis
import rq
import rq.worker
import yaml

# for `API` and `API_VER`, see the APIService definition.
API = "custom.metrics.k8s.io"
API_VER = "v1beta1"


def queue_func(*args, **kwargs):
    """
    This is the job-processing function run by workers.

    For test purposes, it simply sleeps for the specified time.
    """
    logging.basicConfig(format="[%(asctime)-15s] [%(levelname)s] %(message)s")
    logger = logging.getLogger("queue_func")
    logger.setLevel("INFO")
    logger.info(
        "Processing queue job with args=%s and kwargs=%s", args, kwargs
    )
    if "sleep" in kwargs:
        time.sleep(int(kwargs["sleep"]))
    return {
        "queue_func_result": {
            "args": args,
            "kwargs": kwargs,
        },
    }


def worker_info(worker) -> dict:
    content: Dict[str, Any] = {
        "queues": [queue.name for queue in worker.queues],
    }
    for attr in [
        "birth_date",
        # "current_job",
        "failed_job_count",
        "hostname",
        "last_heartbeat",
        "name",
        "pid",
        # "queues",
        "state",
        "successful_job_count",
        "total_working_time",
    ]:
        try:
            content[attr] = getattr(worker, attr)
        except AttributeError:
            content[attr] = "<<attr not found>>"
    return content


def queue_job_count(queue, job_statuses: List[str]) -> int:
    """
    This function counts jobs that in the given state(s) for a given queue.
    """
    jobs = [
        job_id
        for job_id in queue.job_ids
        if queue.fetch_job(job_id).get_status(refresh=True) in job_statuses
    ]
    return len(jobs)


def queue_busy_workers(queue: str) -> List[str]:
    """
    This function counts the number of busy workers for a given queue.
    """
    return [
        worker.name
        for worker in rq.Worker.all(queue=queue)
        if worker.state == "busy"
    ]


def create_apiserver_app(config={}):
    """
    This function is called by gunicorn and creates a Flask app.
    """
    app = flask.Flask(__name__)
    app.config.update(config)

    app.redis_host = os.environ.get("REDIS_HOST", "redis-server")
    app.redis_port = int(os.environ.get("REDIS_PORT", "6379"))
    app.redis_conn = redis.StrictRedis(
        host=app.redis_host,
        port=app.redis_port,
    )

    log_level = os.environ.get("LOGLEVEL", "INFO")
    logging.basicConfig(format="[%(asctime)-15s] [%(levelname)s] %(message)s")
    app.logger.setLevel(log_level)
    app.logger.info("Redis config: host=%s:%s", app.redis_host, app.redis_port)

    @app.route("/")
    def path_root():
        return {"status": "OK"}, 200

    @app.route("/queues/<queue_name>/jobs")
    def path_queue_jobs(queue_name):
        queue = rq.Queue(queue_name, connection=app.redis_conn)
        jobs = {}
        for job_id in queue.job_ids:
            job = queue.fetch_job(job_id)
            if job is None:
                jobs[job_id] = {"error": "job not found"}
            else:
                jobs[job_id] = {
                    "id": job.id,
                    "result": job.result,
                    "status": job.get_status(),
                }
        return {"jobs": jobs}, 200

    @app.route("/queues/<queue_name>/length")
    def path_queue_length(queue_name):
        queue = rq.Queue(queue_name, connection=app.redis_conn)
        return {"length": len(queue)}, 200

    @app.route("/queues/<queue_name>/counts")
    def path_queue_counts(queue_name):
        queue = rq.Queue(queue_name, connection=app.redis_conn)
        counts = collections.defaultdict(lambda: 0)
        for job_id in queue.job_ids:
            job = queue.fetch_job(job_id)
            if job is None:
                continue
            status = job.get_status(refresh=True)
            counts[status] += 1
        return {"counts": counts}, 200

    @app.route("/queues/<queue_name>/enqueue", methods=["POST"])
    def path_queue_enqueue(queue_name):
        sync = flask.request.args.get("sync", "false").lower()
        is_async = not yaml.safe_load(sync)
        queue = rq.Queue(
            queue_name,
            is_async=is_async,
            connection=app.redis_conn,
        )
        job = queue.enqueue(
            queue_func,
            **flask.request.get_json(),
        )
        return {
            "job": {
                "id": job.id,
                "result": job.result,  # None for async job
                "status": job.get_status(),
            },
        }, 200

    @app.route("/queues/<queue_name>/jobs/<job_id>")
    def path_queue_job(queue_name, job_id):
        queue = rq.Queue(queue_name, connection=app.redis_conn)
        job = queue.fetch_job(job_id)
        if job is None:
            return {"error": "job not found"}, 404
        return {
            "job": {
                "id": job.id,
                "result": job.result,
                "status": job.get_status(),
            },
        }, 200

    @app.route("/queues/<queue_name>/workers")
    def path_queue_workers(queue_name):
        queue = rq.Queue(queue_name, connection=app.redis_conn)
        redis_workers = rq.Worker.all(queue=queue)
        workers = [worker_info(worker) for worker in redis_workers]
        return {"workers": workers}, 200

    @app.route("/workers")
    def path_workers():
        redis_workers = rq.Worker.all(connection=app.redis_conn)
        workers = [worker_info(worker) for worker in redis_workers]
        return {"workers": workers}, 200

    return app


def create_metrics_exporter_app(config={}):
    """
    This function is called by gunicorn and creates a Flask app.
    """
    app = flask.Flask(__name__)
    app.config.update(config)

    app.redis_connections: Dict[str, redis.StrictRedis] = {}

    def get_redis_connection(host: str, port: int) -> redis.StrictRedis:
        key = f"{host}:{port}"
        conn = app.redis_connections.get(key, None)
        if conn is None:
            conn = redis.StrictRedis(host=host, port=port)
            app.redis_connections[key] = conn

        return conn

    app.get_redis_connection = get_redis_connection

    log_level = os.environ.get("LOGLEVEL", "INFO")
    logging.basicConfig(format="[%(asctime)-15s] [%(levelname)s] %(message)s")
    app.logger.setLevel(log_level)

    base_path = f"/apis/{API}/{API_VER}"
    metric_path = (
        f"{base_path}/namespaces/<namespace>/deployments.apps/<deployment_app>"
        "/<metric>"
    )

    def redisqueue_length(namespace, deployment_app, metric):
        """
        Return a metric that indicates how many redis queue workers there
        should be. This needs to include 2 components:
        - the number of queued jobs: more queued jobs, more workers needed
        - the number of busy workers: don't scale down and kill busy workers
        """
        now = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        content = {
            "kind": "MetricValueList",
            "apiVersion": f"{API}/{API_VER}",
            "metadata": {"selflink": f"{base_path}/"},
            "items": [
                {
                    "describedObject": {
                        "kind": "Deployment",
                        "namespace": namespace,
                        "name": deployment_app,
                        "apiVersion": "apps/v1",
                    },
                    "metricName": "count",
                    "timestamp": now,
                    "value": "0",
                },
            ],
        }

        try:
            # For `metricLabelSelector`, see `matchLabels` in rq-worker-*.yaml.
            mls = flask.request.args.get("metricLabelSelector")

            if not mls.startswith("queues="):
                return content, 200

            # Contact the redis server in the given namespace. This is because
            # Kubernetes objectes of kind APIService are not namespaced, so the
            # custom metrics server in one namespace needs to be able to query
            # the redis server in any other namespace.
            redis_conn = app.get_redis_connection(
                f"redis-server.{namespace}.svc.cluster.local",
                6379,
            )
            all_busy_workers: List[str] = []
            qinfo = {}
            value = 0
            for qname in mls[7:].split("-"):
                queue = rq.Queue(qname, connection=redis_conn)
                job_count = queue_job_count(queue, ["queued"])
                busy_workers = queue_busy_workers(queue)
                qinfo[qname] = {
                    "queued_job_count": job_count,
                    "busy_workers": busy_workers,
                }
                value += job_count
                all_busy_workers += busy_workers

            # Since rq workers may service more than one queue, avoid counting
            # workers more than once.
            all_busy_workers = list(set(all_busy_workers))
            value += len(all_busy_workers)

            content["items"][0]["_debug"] = {
                "all_busy_workers": all_busy_workers,
                "queue_info": qinfo,
            }
            content["items"][0]["value"] = str(value)

        except Exception as exc:
            app.logger.warning("Swallowed an Exception: %s", str(exc))
            content["items"][0]["_debug"] = {"error": str(exc)}

        # Always return HTTP 200, even after catching an exception, so that the
        # error can be easily seen with:
        # kubectl get --raw "/apis/custom.metrics.k8s.io/v1beta1/..."
        return content, 200

    metrics = {
        "redisqueue_length": redisqueue_length,
    }

    @app.route(f"{base_path}")
    @app.route(f"{base_path}/")
    def path_root():
        return {
            "kind": "APIResourceList",
            "apiVersion": "v1",
            "groupVersion": f"{API}/{API_VER}",
            "resources": [
                {
                    "name": f"deployments.apps/{metric_name}",
                    "singularName": "",
                    "namespaced": True,
                    "kind": "MetricValueList",
                    "verbs": ["get"],
                }
                for metric_name in sorted(metrics.keys())
            ],
        }, 200

    @app.route(metric_path)
    def path_deploymentsapps_metric(namespace, deployment_app, metric):
        func = metrics.get(metric, None)
        if func is None:
            content = {
                "kind": "MetricValueList",
                "apiVersion": f"{API}/{API_VER}",
                "metadata": {"selflink": f"{base_path}/"},
                "items": [],
                "error": "metric not found",
            }
            return content, 200

        return func(namespace, deployment_app, metric)

    return app


class MyWorker(rq.worker.SimpleWorker):
    """
    This class is here in case worker customisations are needed.
    """
