# File: metranova/utils/hostname_formatter.py

import logging
import re
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


class HostnameFormatter:
    """
    High-performance hostname formatting with pre-compiled patterns.

    Formats are defined in YAML and compiled once on init.
    """

    def __init__(self, format_definitions: Dict[str, Any]):
        """
        Initialize formatter with format definitions from YAML.

        Args:
            format_definitions: Dict from _hostname_formats in YAML
        """
        self.formats = {}
        self.default_format = "short"

        # Pre-compile all regex patterns
        for name, config in format_definitions.items():
            format_type = config.get("type")

            if format_type == "regex":
                pattern = config.get("pattern")
                try:
                    compiled = re.compile(pattern)
                    self.formats[name] = {
                        "type": "regex",
                        "compiled": compiled,
                        "group": config.get("group", 1),
                        "join": config.get("join", None),
                    }
                except re.error as e:
                    logger.error(f"Invalid regex pattern for format '{name}': {e}")
                    continue

            elif format_type == "split":
                self.formats[name] = {
                    "type": "split",
                    "delimiter": config.get("delimiter", "."),
                    "index": config.get("index", 0),
                }

            elif format_type == "replace":
                pattern = config.get("pattern", "")
                try:
                    compiled = re.compile(pattern) if pattern else None
                    self.formats[name] = {
                        "type": "replace",
                        "compiled": compiled,
                        "pattern": pattern,
                        "replacement": config.get("replacement", ""),
                    }
                except re.error as e:
                    logger.error(f"Invalid regex pattern for format '{name}': {e}")
                    continue

            elif format_type == "none":
                self.formats[name] = {"type": "none"}

            else:
                logger.warning(
                    f"Unknown hostname format type '{format_type}' for '{name}'"
                )

        logger.info(
            f"Initialized HostnameFormatter with {len(self.formats)} formats: {list(self.formats.keys())}"
        )

    def format(self, hostname: str, format_name: Optional[str] = None) -> str:
        """
        Format hostname according to specified format.

        This is the hot path - optimized for speed!

        Args:
            hostname: Input hostname string
            format_name: Name of format to apply (from _hostname_formats)

        Returns:
            Formatted hostname string
        """
        if not hostname:
            return hostname

        # Use default format if none specified
        if format_name is None:
            format_name = self.default_format

        # No format specified - return as-is
        if format_name is None or format_name == "fqdn":
            return hostname

        # Get format config
        format_config = self.formats.get(format_name)
        if not format_config:
            logger.warning(
                f"Unknown hostname format '{format_name}', using original value"
            )
            return hostname

        format_type = format_config["type"]

        if format_type == "split":
            try:
                parts = hostname.split(format_config["delimiter"])
                index = format_config["index"]
                if -len(parts) <= index < len(parts):
                    return parts[index]
                return hostname
            except (IndexError, AttributeError):
                logger.debug(
                    f"Split failed for hostname '{hostname}' with format '{format_name}'"
                )
                return hostname

        elif format_type == "regex":
            try:
                match = format_config["compiled"].match(hostname)
                if not match:
                    logger.debug(
                        f"Regex no match for hostname '{hostname}' with format '{format_name}'"
                    )
                    return hostname

                group = format_config["group"]

                # Single group
                if isinstance(group, int):
                    return match.group(group)

                # Multiple groups with join
                if isinstance(group, str) and "," in group:
                    groups = [int(g.strip()) for g in group.split(",")]
                    values = [match.group(g) for g in groups]
                    join_char = format_config.get("join", "")
                    return join_char.join(values)

                return match.group(1)

            except (IndexError, AttributeError) as e:
                logger.debug(f"Regex extraction failed for hostname '{hostname}': {e}")
                return hostname

        elif format_type == "replace":
            try:
                if format_config["compiled"]:
                    return format_config["compiled"].sub(
                        format_config["replacement"], hostname
                    )
                else:
                    return hostname.replace(
                        format_config["pattern"], format_config["replacement"]
                    )
            except Exception as e:
                logger.debug(f"Replace failed for hostname '{hostname}': {e}")
                return hostname

        elif format_type == "none":
            return hostname

        # Unknown type
        return hostname

    def set_default_format(self, format_name: str):
        """Set the default format to use when none specified."""
        self.default_format = format_name
