"""
Provide a Flask app
"""

import datetime
import logging
import os
import time

import flask
import redis
import rq
import rq.worker
import yaml


def queue_func(*args, **kwargs):
    """
    This is the job-processing function run by workers.

    For test purposes, it simply sleeps for the specified time.
    """
    logging.basicConfig(format="[%(asctime)-15s] [%(levelname)s] %(message)s")
    logger = logging.getLogger("queue_func")
    logger.setLevel("INFO")
    logger.info("Processing queue job with args=%s and kwargs=%s", args, kwargs)
    if "sleep" in kwargs:
        time.sleep(int(kwargs["sleep"]))
    return {
        "queue_func_result": {
            "args": args,
            "kwargs": kwargs,
        },
    }


def base_app(config: dict):
    """
    This function creates a base Flask app.
    """
    app = flask.Flask(__name__)
    app.config.update(config)

    app.redis_host = os.environ.get("REDIS_HOST", "redis-server")
    app.redis_port = int(os.environ.get("REDIS_PORT", "6379"))
    app.redis_conn = redis.StrictRedis(
        host=app.redis_host,
        port=app.redis_port,
    )

    redis_queues = os.environ.get("REDIS_QUEUES", "high,low")
    app.redis_queue_names = [x.strip() for x in redis_queues.split(",")]

    log_level = os.environ.get("LOGLEVEL", "INFO")
    logging.basicConfig(format="[%(asctime)-15s] [%(levelname)s] %(message)s")
    app.logger.setLevel(log_level)
    app.logger.info(
        "Redis config: host=%s:%s queues=[%s]",
        app.redis_host, app.redis_port, ",".join(app.redis_queue_names)
    )

    return app


def create_apiserver_app(config={}):
    """
    This function is called by gunicorn and creates a Flask app.
    """
    app = base_app(config)

    @app.route("/")
    def path_root():
        return {"status": "OK"}, 200

    @app.route("/queues")
    def path_queues_list():
        return {"queues": sorted(app.redis_queue_names)}, 200

    @app.route("/queues/<queue_name>/length")
    def path_queue_length(queue_name):
        if queue_name not in app.redis_queue_names:
            return {"error": "queue not found"}, 404
        queue = rq.Queue(queue_name, connection=app.redis_conn)
        return {"length": len(queue)}, 200

    @app.route("/queues/<queue_name>/enqueue", methods=["POST"])
    def path_queues_enqueue(queue_name):
        if queue_name not in app.redis_queue_names:
            return {"error": "queue not found"}, 404
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
    def path_job(queue_name, job_id):
        if queue_name not in app.redis_queue_names:
            return {"error": "queue not found"}, 404
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

    return app


def create_metrics_exporter_app(config={}):
    """
    This function is called by gunicorn and creates a Flask app.
    """
    app = base_app(config)

    api = "custom.metrics.k8s.io"
    apiVer = "v1beta1"
    path_base = f"/apis/{api}/{apiVer}"

    @app.route(f"{path_base}/")
    def path_root():
        return {"status": "healthy"}, 200

    @app.route(
        f"{path_base}/namespaces/<namespace>/deployments.apps/<deployment_app>"
        "/<metric>"
    )
    def path_deploymentsapps_metric(namespace, deployment_app, metric):
        mls = flask.request.args.get("metricLabelSelector")
        # see matchLabels in rq-worker-*.yaml.
        queue_length = 0
        queue_name = "?"
        if mls.startswith("queue="):
            queue_name = mls[6:]
            if queue_name in app.redis_queue_names:
                queue = rq.Queue(queue_name, connection=app.redis_conn)
                queue_length = len(queue)

        now = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        content = {
            "kind": "MetricValueList",
            "apiVersion": f"{api}/{apiVer}",
            "metadata": {"selflink": f"{path_base}/"},
            "items": [
                {
                    "describedObject": {
                        "kind": "Service",
                        "namespace": namespace,
                        "name": f"rq-worker-{queue_name}",
                        "apiVersion": f"/{apiVer}",
                    },
                    "metricName": "count",
                    "timestamp": now,
                    "value": str(queue_length),
                },
            ],
        }
        return content, 200

    return app


class MyWorker(rq.worker.SimpleWorker):
    """
    This class is here in case worker customisations are needed.
    """
