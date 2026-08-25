from typing import Any

import sentry_sdk
from kafka import KafkaAdminClient, KafkaProducer
from kafka.errors import KafkaTimeoutError
from osprey.engine.executor.execution_context import ExecutionResult
from osprey.worker.lib.osprey_shared.logging import get_logger
from osprey.worker.sinks.sink.output_sink import BaseOutputSink

logger = get_logger()


class EmptyBootstrapServersException(Exception):
    """Exception that is raised whenever the server list provided to KafkaOutputSink is empty."""


class InvalidOutputTopicException(Exception):
    """Exception that is raised whenever the output topic passed to KafkaOutputSink is empty."""


class KafkaOutputSink(BaseOutputSink):
    """An output sink that sends the extracted features to a given kafka topic."""

    def __init__(
        self,
        output_topic: str,
        bootstrap_servers: list[str],
        client_id: str | None,
        auto_create_topic: bool = True,
        num_partitions: int = 1,
        replication_factor: int = 1,
    ) -> None:
        if len(bootstrap_servers) == 0:
            raise EmptyBootstrapServersException()

        if output_topic == '':
            raise InvalidOutputTopicException()

        self.logger = get_logger('KafkaOutputSink')

        self._bootstrap_servers = bootstrap_servers
        self._output_topic = output_topic
        self._num_partitions = num_partitions
        self._replication_factor = replication_factor
        self.client_id = client_id

        self.logger.info(f'Creating Kafka producer with client id {client_id}')

        config = {
            'bootstrap_servers': self._bootstrap_servers,
            'client_id': self.client_id,
            'linger_ms': 10,
            'retries': 10,
            'socket_connection_setup_timeout_ms': 30_000,
            'acks': 'all',
        }

        self._kafka_producer = KafkaProducer(**config)

        if auto_create_topic:
            self.ensure_topic()

    def ensure_topic(self) -> None:
        """Create the Kafka topic if it does not yet exist."""
        admin_client = KafkaAdminClient({'bootstrap.servers': ','.join(self._bootstrap_servers)})

        try:
            topics = admin_client.list_topics()
        except Exception as e:
            self.logger.error(f'Error listing topics, unable to ensure topic: {e}')
            return

        if self._output_topic in topics:
            return

        try:
            topic = {
                self._output_topic: {
                    'num_partitions': self._num_partitions,
                    'replication_factor': self._replication_factor,
                }
            }

            admin_client.create_topics([topic])
            self.logger.info(f'Created topic {self._output_topic}')
        except Exception as e:
            self.logger.error(f'Error creating topic, unable to ensure topic: {e}')

    def will_do_work(self, result: ExecutionResult) -> bool:
        return True

    def push(self, result: ExecutionResult) -> None:
        kafka_future: Any = self._kafka_producer.send(
            topic=self._output_topic, value=result.extracted_features_json.encode('utf-8')
        )
        kafka_future.add_errback(self.push_err_to_sentry)

    @classmethod
    def push_err_to_sentry(cls, e: Exception) -> None:
        logger.error(f'exception raised when pushing event to kafka: {str(e)}')
        sentry_sdk.capture_exception(error=e)

    def flush(self, timeout: float = 30) -> None:
        try:
            self._kafka_producer.flush(timeout)
        except KafkaTimeoutError as e:
            self.logger.error(f'Flush failed: {e}')

    def stop(self) -> None:
        pass
