import yaml


def _load_connections(config_path: str) -> dict:
    with open(config_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return {c["id"]: c for c in data.get("connections", [])}


def get_jdbc_reader(spark, dbutils, connection_id: str, sql_query: str, config_path: str):
    """
    Return a Spark DataFrame read via JDBC for the given connection and query.

    Credentials are fetched from Databricks Secrets using secret_scope/secret_key
    defined in connections.yml.  Never store passwords in YAML or code.

    config_path: absolute path to connections.yml (e.g. /Workspace/Repos/.../connections.yml)
    """
    connections = _load_connections(config_path)

    if connection_id not in connections:
        raise ValueError(
            f"Connection '{connection_id}' não encontrada em {config_path}. "
            f"IDs disponíveis: {list(connections.keys())}"
        )

    conn = connections[connection_id]

    password = dbutils.secrets.get(scope=conn["secret_scope"], key=conn["secret_key"])
    jdbc_url = (
        f"jdbc:{conn['subprotocol']}://{conn['host']}:{conn['port']}/{conn['database']}"
    )

    return (
        spark.read
        .format("jdbc")
        .option("url",      jdbc_url)
        .option("dbtable",  f"({sql_query}) AS q")
        .option("user",     conn["user"])
        .option("password", password)
        .option("driver",   conn["driver"])
        .load()
    )
