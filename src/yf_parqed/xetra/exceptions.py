class XetraSchemaUnknownError(ValueError):
    """Raised when no registered schema matches the fields in a trade record."""

    def __init__(self, actual_fields: list[str]):
        self.actual_fields = actual_fields
        super().__init__(
            f"No registered schema matches fields: {sorted(actual_fields)}"
        )
