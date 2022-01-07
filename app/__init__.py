"""
Provide a Flask app
"""

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
    if "sleep" in kwargs:
        time.sleep(int(kwargs["sleep"]))
    return {
        "queue_func_result": {
            "args": args,
            "kwargs": kwargs,
        },
    }


def create_app(config={}):
    """
    This function is called by gunicorn and creates a Flask app.
    """
    app = flask.Flask(__name__)
    app.config.update(config)

    redis_host = os.environ.get("REDIS_HOST", "redis-server")
    redis_port = int(os.environ.get("REDIS_PORT", "6379"))
    redis_conn = redis.StrictRedis(host=redis_host, port=redis_port)

    redis_queues = os.environ.get("REDIS_QUEUES", "high,low")
    redis_queue_names = [x.strip() for x in redis_queues.split(",")]

    log_level = os.environ.get("LOGLEVEL", "INFO")
    logging.basicConfig(format="[%(asctime)-15s] [%(levelname)s] %(message)s")
    app.logger.setLevel(log_level)
    app.logger.info(
        "Redis config: host=%s:%s queues=[%s]",
        redis_host, redis_port, ",".join(redis_queue_names)
    )

    @app.route("/")
    def path_root():
        queue_name = {
            "name": "queue_name",
            "in": "path",
            "required": True,
            "schema": {
                "type": "string",
            },
        }
        content = {
            "paths": {
                "/queues": {
                    "get": {
                        "summary": "List configured queues",
                    },
                },
                "/queues/{queue_name}/length": {
                    "get": {
                        "summary": "Get length of named queue",
                        "parameters": [queue_name],
                    },
                },
                "/queues/{queue_name}/enqueue": {
                    "post": {
                        "summary": "Enqueue job",
                        "parameters": [
                            queue_name,
                            {
                                "name": "sync",
                                "in": "query",
                                "required": False,
                                "schema": {
                                    "type": "bool",
                                    "default": True,
                                },
                            },
                        ],
                    },
                },
                "/queues/{queue_name}/jobs/{job_id}": {
                    "get": {
                        "summary": "Get job details",
                        "parameters": [queue_name],
                    },
                },
            },
        }
        return content, 200

    @app.route("/metrics")
    def path_metrics():
        mid = "yourappnamehere_redis_queue_length"
        response = (
            [
                f"# TYPE {mid} gauge",
                f"# HELP {mid} Redis queue length",
            ]
            + [
                f'{mid}{{queue="{queue_name}"}} '
                f"{len(rq.Queue(queue_name, connection=redis_conn))}"
                for queue_name in redis_queue_names
            ]
            + ["# EOF\n"]
        )
        mime = "application/openmetrics-text; version=1.0.0; charset=utf-8"
        return ("\n".join(response), 200, {"Content-Type": mime})

    @app.route("/queues")
    def path_queues_list():
        return {"queues": sorted(redis_queue_names)}, 200

    @app.route("/queues/<queue_name>/length")
    def path_queue_length(queue_name):
        if queue_name not in redis_queue_names:
            return {"error": "queue not found"}, 404
        queue = rq.Queue(queue_name, connection=redis_conn)
        return {"length": len(queue)}, 200

    @app.route("/queues/<queue_name>/enqueue", methods=["POST"])
    def path_queues_enqueue(queue_name):
        if queue_name not in redis_queue_names:
            return {"error": "queue not found"}, 404
        sync = flask.request.args.get("sync", "false").lower()
        is_async = not yaml.safe_load(sync)
        queue = rq.Queue(queue_name, is_async=is_async, connection=redis_conn)
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
        if queue_name not in redis_queue_names:
            return {"error": "queue not found"}, 404
        queue = rq.Queue(queue_name, connection=redis_conn)
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


class MyWorker(rq.worker.SimpleWorker):
    """
    This class is here in case worker customisations are needed.
    """
