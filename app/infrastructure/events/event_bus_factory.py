"""
Factory for creating event bus instances from configuration.
"""

import logging
from typing import Optional

from app.infrastructure.config_loader import LoadEnvironmentVariables
from .redis_event_bus import RedisEventBus


class EventBusFactory:
    """Factory for creating event bus instances."""

    @staticmethod
    def create_from_env(
        env_config: LoadEnvironmentVariables,
        logger: Optional[logging.Logger] = None
    ) -> RedisEventBus:
        """
        Create Redis event bus from environment configuration.

        Args:
            env_config: Environment configuration with Redis connection params
            logger: Optional logger instance

        Returns:
            Configured RedisEventBus instance

        Raises:
            ImportError: If redis-py is not installed
            redis.ConnectionError: If cannot connect to Redis

        Example:
            >>> from app.infrastructure.config_loader import LoadEnvironmentVariables
            >>> env_config = LoadEnvironmentVariables(".env")
            >>> event_bus = EventBusFactory.create_from_env(env_config)
            >>> event_bus.publish(event)
        """
        if logger is None:
            logger = logging.getLogger(__name__)

        logger.info(
            f"Creating Redis event bus connection to "
            f"{env_config.REDIS_HOST}:{env_config.REDIS_PORT} "
            f"[APP_TAG: {env_config.APP_TAG}]"
        )

        return RedisEventBus(
            host=env_config.REDIS_HOST,
            port=env_config.REDIS_PORT,
            db=env_config.REDIS_DB,
            password=env_config.REDIS_PASSWORD,
            app_tag=env_config.APP_TAG,
            logger=logger
        )

    @staticmethod
    def create_from_params(
        host: str = "localhost",
        port: int = 6379,
        db: int = 0,
        password: Optional[str] = None,
        logger: Optional[logging.Logger] = None
    ) -> RedisEventBus:
        """
        Create Redis event bus from explicit parameters.

        Args:
            host: Redis server hostname
            port: Redis server port
            db: Redis database number
            password: Optional Redis password
            logger: Optional logger instance

        Returns:
            Configured RedisEventBus instance

        Example:
            >>> event_bus = EventBusFactory.create_from_params(
            ...     host="redis-server",
            ...     port=6379
            ... )
        """
        if logger is None:
            logger = logging.getLogger(__name__)

        return RedisEventBus(
            host=host,
            port=port,
            db=db,
            password=password,
            logger=logger
        )
