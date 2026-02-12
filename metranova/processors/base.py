import logging
from typing import Any, Dict, Iterator

from metranova.utils.hostname_formatter import HostnameFormatter

logger = logging.getLogger(__name__)


class BaseProcessor:
    def __init__(self, pipeline):
        # Initialize parent class - need this for multiple inheritance
        super().__init__()

        # setup logger
        self.logger = logger

        # assign parent pipeline
        self.pipeline = pipeline

        # hostname formatter will be sent by pipeline after loading yaml files
        self.hostname_formatter: HostnameFormatter = None

        # override in child class
        self.match_fields = []

        # override in child class
        self.required_fields = []

    def format_hostname(self, hostname: str, format_name: str = None) -> str:
        """
        Format hostname using configured formatter.

        This delegates to the HostnameFormatter instance which has
        pre-compiled regex patterns for fast execution.

        Args:
            hostname: Input hostname
            format_name: Format to apply (from _hostname_formats in YAML)

        Returns:
            Formatted hostname
        """
        if self.hostname_formatter is None:
            logger.warning(
                "HostnameFormatter is not initialized, using original hostname."
            )
            return hostname

        return self.hostname_formatter.format(hostname, format_name)

    def has_required_fields(self, value: dict) -> bool:
        """Check if the message contains all required fields"""
        # NOTE: Call this in build_message instead of match_message to allow more detailed logging
        # This means the message matched the processor, but there is a problem with the content
        for fields in self.required_fields:
            v = None
            for field in fields:
                if v is None:
                    v = value.get(field, None)
                else:
                    v = v.get(field, None)
                if v is None:
                    self.logger.error(
                        f"Missing required field '{field}' in message value"
                    )
                    return False
        return True

    def has_match_field(self, value: dict) -> bool:
        if not self.match_fields:
            return True  # No match fields defined, always match
        for path in self.match_fields:
            current = value
            for key in path:
                if not isinstance(current, dict) or key not in current:
                    break
                current = current[key]
            else:  # only executed if inner loop did not break
                if current is not None:
                    return True
        return False

    def match_message(self, value: dict) -> bool:
        """Determine if this processor should handle the given message"""
        return self.has_match_field(value)

    def build_message(
        self, value: dict, msg_metadata: dict
    ) -> Iterator[Dict[str, Any]]:
        """Build message dictionary for ClickHouse insertion"""
        raise NotImplementedError("Subclasses should implement this method")
