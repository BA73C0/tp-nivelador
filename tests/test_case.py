import time
from typing import Callable

from utils import shell_cmd
from utils import docker


class TestCase:
    title: str = ""
    error_hint: str = ""
    has_service_logs: bool = True

    @classmethod
    def with_docker_run(
        cls,
        docker_compose_path: str,
        test_callback: Callable,
    ):
        try:
            docker.up(docker_compose_path)
            return test_callback()
        except:
            service_logs_path = cls.get_service_logs_file_path()
            if service_logs_path:
                with open(service_logs_path, "w") as logs_file:
                    logs = [
                        line + "\n" for line in docker.get_logs(docker_compose_path)
                    ]
                    logs_file.writelines(logs)
            raise
        finally:
            docker.down(docker_compose_path)

    @classmethod
    def get_service_logs_file_path(cls) -> str | None:
        return "failed_test.log" if cls.has_service_logs else None

    @staticmethod
    def await_net_io_stop(service_name: str, pooling_await_seconds=1):
        last_net_recv = ""
        last_net_sent = ""
        while True:
            [net_recv, net_sent] = docker.get_container_net_io(service_name)
            if last_net_recv == net_recv and last_net_sent == net_sent:
                return
            last_net_recv = net_recv
            last_net_sent = net_sent
            time.sleep(pooling_await_seconds)

    @staticmethod
    def test() -> None:
        raise NotImplementedError("Test cases require a test function")
