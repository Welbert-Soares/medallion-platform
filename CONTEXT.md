# Medallion Platform

Metadata-driven data platform: declared Source Systems are ingested as-is into a raw Layer and refined through bronze, silver and gold.

## Language

### Sources

**Source System**:
An external system or domain that data comes from (e.g. weather, ibge, orders); its name is the schema name in every Layer.
_Avoid_: group, domain, name, container

**Catalog Structure**:
The declared set of Layers, Source Systems and access grants; anything not declared there does not exist on the platform.
_Avoid_: infra config, setup

**Ingestion**:
One source object declared in config that lands in exactly one raw table (e.g. `weather_sao_paulo`).
_Avoid_: job, load, pipeline

**Connection**:
The address and credential reference of an external database that sql/table Ingestions read from.
_Avoid_: linked service, datasource

**Watermark**:
How far an Ingestion has already landed in Raw: the highest value of its incremental column, or the fingerprint of its source file; the next Run reads only beyond it.
_Avoid_: checkpoint, cursor

**Ingestion State**:
What the platform has learned by running an Ingestion (its Watermark and last Run); never declared in config.
_Avoid_: ingestion config, metadata

### Platform and Layers

**Controller**:
The part of the platform that declares, configures and records: config, Catalog Structure, Ingestion State and the execution log. It holds no business data and is not a Layer.
_Avoid_: control layer, orchestrator

**Layer**:
A stage of data refinement, each backed by its own catalog: raw, bronze, silver, gold.
_Avoid_: container, zone, tier

**Raw**:
The immutable landing Layer: every load is appended as-is and stamped with the Run that produced it; duplicates are allowed here.
_Avoid_: staging, landing table

### Execution

**Run**:
One execution of the platform job end to end.
_Avoid_: execution, batch

**Ingestion Run**:
One Ingestion executed inside a Run.
_Avoid_: task, execution
