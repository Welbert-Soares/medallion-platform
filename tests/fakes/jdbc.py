from medallion.ingestion.run import ReadResult


class FakeSecrets:
    def __init__(self, secrets: dict[tuple[str, str], str]):
        self.secrets = secrets
        self.requested: list[tuple[str, str]] = []

    def get(self, scope, key) -> str:
        self.requested.append((scope, key))
        return self.secrets[(scope, key)]


class FakeJdbcClient:
    """Returns canned rows; reports the max of the watermark column like the real client."""

    def __init__(self, rows):
        self.rows = rows
        self.requests = []

    def read(self, request) -> ReadResult:
        self.requests.append(request)
        col = request.watermark_column
        values = [r[col] for r in self.rows if col and r.get(col) is not None]
        return ReadResult(list(self.rows), len(self.rows), max(values) if values else None)
