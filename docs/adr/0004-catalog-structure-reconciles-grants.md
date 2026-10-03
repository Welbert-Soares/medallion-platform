# Catalog Structure reconciles grants, including revokes

Applying the Catalog Structure makes Unity Catalog match the YAML exactly: undeclared grants on managed catalogs/schemas are revoked, not just left alone. A grant added by hand in the UI will disappear on the next deploy; declare it in `catalog_structure.yml` instead. A dry-run mode prints the diff before applying. Catalogs, schemas and volumes are only created, never dropped automatically.
