"""Exceptions shared across document task registration and implementations."""


class SearchIndexLockError(Exception):
    """Raised when the search index file lock cannot be acquired within the timeout."""
