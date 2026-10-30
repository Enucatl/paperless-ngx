"""Document data types can be imported without initializing the application."""

__all__ = ["parser_check"]


def __getattr__(name: str) -> object:
    """Keep the legacy check export without loading Django for task DTOs."""
    if name == "parser_check":
        from documents.checks import parser_check

        return parser_check
    raise AttributeError(f"module 'documents' has no attribute {name!r}")
