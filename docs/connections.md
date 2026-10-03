# Connections and their secrets

sql/table Ingestions read external databases through a **Connection** declared in
`config/platform/connections.yml`. A Connection holds only the address and a *reference* to its
password (`secret_scope` + `secret_key`); the password itself lives in a Databricks-backed secret
scope and is read at run time. It never goes into YAML, Controller tables, logs or Git.

## What is automatic

- The deploy runs `medallion-secret-scopes`, which creates every `secret_scope` referenced by a
  declared Connection (today: `medallion-platform`). It is idempotent and never writes values.
- `sync_config` mirrors Connections to `medallion_controller.controller.connections`
  (address + secret reference only).

## What the maintainer does by hand

Put the password in the scope once per Connection, from a terminal authenticated to the
workspace (the CLI prompts for the value, so it is not left in shell history):

```bash
databricks secrets put-secret medallion-platform postgres_password
```

Check that the key exists (values are never shown):

```bash
databricks secrets list-secrets medallion-platform
```

Never paste a password into a chat, an issue, a PR or a commit.

## Adding a Connection

1. Declare it in `config/platform/connections.yml` with its `secret_scope` and `secret_key`.
2. Merge: the deploy creates the scope if it is new.
3. Put the password as above.
4. Point an Ingestion at it with `source.connection_id` and set `active: true`.

The host must be reachable from Databricks serverless compute: `orders_postgres` stays inactive
because its Connection points at `localhost`.
