"""Path filtering shared by consumption APIs and filesystem watchers."""

import os
import re
from pathlib import Path
from typing import Final


class FileFilter:
    """Apply the same path policy to watcher events and API admissions."""

    # Regex patterns for files to always ignore (matched against filename only)
    DEFAULT_IGNORE_PATTERNS: Final[tuple[str, ...]] = (
        r"^\.DS_Store$",
        r"^\.DS_STORE$",
        r"^\._.*",
        r"^desktop\.ini$",
        r"^Thumbs\.db$",
    )

    # Directories to always ignore
    # These are matched by directory name, not full path
    DEFAULT_IGNORE_DIRS: Final[tuple[str, ...]] = (
        ".stfolder",  # Syncthing
        ".stversions",  # Syncthing
        ".localized",  # macOS
        "@eaDir",  # Synology NAS
        ".Spotlight-V100",  # macOS
        ".Trashes",  # macOS
        "__MACOSX",  # macOS archive artifacts
    )

    def __init__(
        self,
        *,
        supported_extensions: frozenset[str],
        ignore_patterns: list[str] | None = None,
        ignore_dirs: list[str] | None = None,
    ) -> None:
        """
        Initialize the consumer filter.

        Args:
            supported_extensions: Set of file extensions to accept (e.g., {".pdf", ".png"}).
            ignore_patterns: Additional regex patterns to ignore (matched against filename).
            ignore_dirs: Additional directory names to ignore (merged with defaults).
        """
        self._supported_extensions = supported_extensions

        # Combine default and user patterns
        all_patterns: list[str] = list(self.DEFAULT_IGNORE_PATTERNS)
        if ignore_patterns:
            all_patterns.extend(ignore_patterns)

        # Combine default and user ignore_dirs
        all_ignore_dirs: list[str] = list(self.DEFAULT_IGNORE_DIRS)
        if ignore_dirs:
            all_ignore_dirs.extend(ignore_dirs)

        self._ignore_dirs = set(all_ignore_dirs)
        self._ignore_entity_regexes = tuple(re.compile(p) for p in all_patterns)

    def __call__(self, change: object, path: str) -> bool:
        """Accept watchfiles callbacks without depending on its event type."""
        return self.matches(path)

    def matches(self, path: str) -> bool:
        """Match path components and basename before checking the extension."""
        # Preserve watchfiles' raw path component matching, including empty parts.
        parts = path.lstrip(os.sep).split(os.sep)  # noqa: PTH206
        if any(part in self._ignore_dirs for part in parts):
            return False
        if any(pattern.search(parts[-1]) for pattern in self._ignore_entity_regexes):
            return False
        path_obj = Path(path)
        return (
            path_obj.is_dir() or path_obj.suffix.lower() in self._supported_extensions
        )
