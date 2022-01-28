"""
Provide a Flask app
"""

import collections
import datetime
import logging
import os
import time
from typing import List

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
    content = {
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

    # A list of all valid queue names
    app.redis_queue_names = ["high", "low"]

    log_level = os.environ.get("LOGLEVEL", "INFO")
    logging.basicConfig(format="[%(asctime)-15s] [%(levelname)s] %(message)s")
    app.logger.setLevel(log_level)
    app.logger.info(
        "Redis config: host=%s:%s queues=[%s]",
        app.redis_host,
        app.redis_port,
        ",".join(app.redis_queue_names),
    )

    return app


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


def queue_busy_worker_count(queue: str) -> int:
    """
    This function counts the number of busy workers for a given queue.
    """
    busy_workers = [
        worker.name
        for worker in rq.Worker.all(queue=queue)
        if worker.state == "busy"
    ]
    return len(busy_workers)


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

    @app.route("/queues/<queue_name>/jobs")
    def path_queue_jobs(queue_name):
        if queue_name not in app.redis_queue_names:
            return {"error": "queue not found"}, 404
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
        if queue_name not in app.redis_queue_names:
            return {"error": "queue not found"}, 404
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
    def path_queue_job(queue_name, job_id):
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

    @app.route("/queues/<queue_name>/workers")
    def path_queue_workers(queue_name):
        if queue_name not in app.redis_queue_names:
            return {"error": "queue not found"}, 404
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
    app = base_app(config)

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
        value = 0
        qnames = "?"
        qinfo = {}
        # For `metricLabelSelector`, see `matchLabels` in rq-worker-*.yaml.
        mls = flask.request.args.get("metricLabelSelector")
        if mls.startswith("queues="):
            qnames = mls[7:]
            for qname in qnames.split("-"):
                if qname not in app.redis_queue_names:
                    qinfo[qname] = {"error": "queue not found"}
                    continue
                queue = rq.Queue(qname, connection=app.redis_conn)
                jobs = queue_job_count(queue, ["queued"])
                busy_workers = queue_busy_worker_count(queue)
                qinfo[qname] = {
                    "queued_jobs": jobs,
                    "busy_workers": busy_workers,
                }
                value += jobs + busy_workers

        now = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        content = {
            "kind": "MetricValueList",
            "apiVersion": f"{API}/{API_VER}",
            "metadata": {"selflink": f"{base_path}/"},
            "items": [
                {
                    "_debug": {
                        "queue_info": qinfo,
                    },
                    "describedObject": {
                        "kind": "Deployment",
                        "namespace": namespace,
                        "name": deployment_app,
                        "apiVersion": "apps/v1",
                    },
                    "metricName": "count",
                    "timestamp": now,
                    "value": str(value),
                },
            ],
        }
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
            return {"error": "metric not found"}, 404

        return func(namespace, deployment_app, metric)

    return app


class MyWorker(rq.worker.SimpleWorker):
    """
    This class is here in case worker customisations are needed.
    """
