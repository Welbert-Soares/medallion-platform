class FakeControllerTables:
    """Keeps the last rows written to each table, as an overwrite would."""

    def __init__(self):
        self.ingestions: list[dict] = []
        self.connections: list[dict] = []

    def replace_ingestions(self, rows) -> None:
        self.ingestions = list(rows)

    def replace_connections(self, rows) -> None:
        self.connections = list(rows)
