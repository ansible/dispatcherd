"""One-shot SQL Server broker demo for two dispatcherd containers."""

import argparse
import json
import os
import uuid
from typing import Any

from dispatcherd import run_service
from dispatcherd.brokers import get_broker
from dispatcherd.config import setup
from dispatcherd.factories import get_publisher_from_settings

DEFAULT_DRIVER = "ODBC Driver 18 for SQL Server"


def _required_env(name: str) -> str:
    value = os.environ.get(name, "")
    if not value:
        raise RuntimeError(f"Set {name} in .env.ms-demo before running the SQL Server demo")
    return value


def _connection_config() -> dict[str, Any]:
    return {
        "server": _required_env("MS_DEMO_SQLSERVER_SERVER"),
        "port": int(os.environ.get("MS_DEMO_SQLSERVER_PORT", "1433")),
        "database": os.environ.get("MS_DEMO_SQLSERVER_DATABASE", "dispatcherd"),
        "user": _required_env("MS_DEMO_SQLSERVER_USER"),
        "password": _required_env("MS_DEMO_SQLSERVER_PASSWORD"),
        "driver": os.environ.get("MS_DEMO_SQLSERVER_DRIVER", DEFAULT_DRIVER),
        "trust_server_certificate": os.environ.get("MS_DEMO_SQLSERVER_TRUST_SERVER_CERTIFICATE", "yes"),
        "connect_timeout_seconds": int(os.environ.get("MS_DEMO_SQLSERVER_CONNECT_TIMEOUT_SECONDS", "15")),
        "query_timeout_seconds": int(os.environ.get("MS_DEMO_SQLSERVER_QUERY_TIMEOUT_SECONDS", "30")),
    }


def _run_id() -> str:
    return _required_env("MS_DEMO_RUN_ID")


def _channel(role: str, run_id: str | None = None) -> str:
    return f"ms_demo_{run_id or _run_id()}_{role}"


def _service_config(role: str) -> dict[str, Any]:
    if role not in {"node1", "node2"}:
        raise ValueError(f"Unknown MS_DEMO_NODE role: {role}")

    channel = _channel(role)
    return {
        "version": 2,
        "brokers": {
            "sql_server": {
                "config": _connection_config(),
                "channels": [channel],
                "default_publish_channel": channel,
                "poll_interval_seconds": 0.1,
            }
        },
        "service": {
            "pool_kwargs": {"min_workers": 1, "max_workers": 2},
            "main_kwargs": {"node_id": f"ms-demo-{role}"},
        },
        "publish": {"default_broker": "sql_server", "default_control_broker": "sql_server"},
    }


def receive_demo_task(run_id: str, message_id: str) -> None:
    """A node 2 task that acknowledges execution on a unique run channel."""
    role = os.environ.get("MS_DEMO_NODE")
    if role != "node2":
        raise RuntimeError(f"Demo task was expected on node2, but ran on {role!r}")
    if run_id != _run_id():
        raise RuntimeError("Demo task run id does not match this stack")

    publisher = get_publisher_from_settings()
    acknowledgment = {
        "run_id": run_id,
        "message_id": message_id,
        "node_id": "ms-demo-node2",
        "worker_pid": os.getpid(),
    }
    try:
        publisher.publish_message(channel=_channel("ack", run_id), message=json.dumps(acknowledgment))
    finally:
        publisher.close()


def run_client() -> None:
    """Publish a task to node2 and wait for its SQL Server acknowledgment."""
    run_id = _run_id()
    message_id = uuid.uuid4().hex
    connection_config = _connection_config()

    publisher = get_broker(
        "sql_server",
        {
            "config": connection_config,
            "channels": [_channel("node1", run_id)],
            "default_publish_channel": _channel("node2", run_id),
            "poll_interval_seconds": 0.1,
        },
    )
    acknowledgment_listener = get_broker(
        "sql_server",
        {
            "config": connection_config,
            "channels": [_channel("ack", run_id)],
            "poll_interval_seconds": 0.1,
        },
    )

    task_message = {
        "task": "tools.demo.ms_sql_demo.receive_demo_task",
        "args": [run_id, message_id],
        "uuid": f"ms-demo-{run_id}-{message_id}",
    }

    try:
        print(f"Sending this message to node2 on SQL Server channel {_channel('node2', run_id)}:")
        print(json.dumps(task_message, indent=2))
        publisher.publish_message(channel=_channel("node2", run_id), message=json.dumps(task_message))
        print(f"Listening for container 2 (node2) on SQL Server acknowledgment channel {_channel('ack', run_id)}...")

        received = list(acknowledgment_listener.process_notify(timeout=45, max_messages=1))
        if not received:
            raise RuntimeError("Timed out waiting for node2 to execute and acknowledge the task")

        _, payload = received[0]
        acknowledgment = json.loads(payload)
        if acknowledgment.get("run_id") != run_id or acknowledgment.get("message_id") != message_id:
            raise RuntimeError("Received an acknowledgment for a different demo run or task")
        if acknowledgment.get("node_id") != "ms-demo-node2":
            raise RuntimeError(f"Expected execution on node2, got {acknowledgment.get('node_id')!r}")

        print("Received acknowledgment from container 2 (node2):")
        print(json.dumps(acknowledgment, indent=2))
        print("PASS: node2 executed the task and acknowledged it " f"(worker pid {acknowledgment['worker_pid']})")
    finally:
        publisher.close()
        acknowledgment_listener.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("service", "client"))
    args = parser.parse_args()

    if args.mode == "service":
        role = _required_env("MS_DEMO_NODE")
        setup(config=_service_config(role))
        run_service()
    else:
        run_client()


if __name__ == "__main__":
    main()
