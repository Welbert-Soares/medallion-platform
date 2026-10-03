# Ingestion config and Ingestion State live in separate tables

`delta_controller.ingestions` is a pure mirror of the YAML and is fully replaced on every sync; `delta_controller.ingestion_state` (Watermark, last Run) is written only by the runtime. Keeping both in one table let the sync MERGE silently reset Watermarks to NULL. Do not merge them back into one table.
