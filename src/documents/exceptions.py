"""Exceptions shared across document task registration and implementations."""


class SearchIndexLockError(Exception):
    """Raised when the search index file lock cannot be acquired within the timeout."""


class ConsumerError(Exception):
    """Report consumption failures without importing processing code to unpickle."""
