CREATE EXTENSION IF NOT EXISTS "postgis";

CREATE TABLE IF NOT EXISTS "datasets" (
    "table_name" TEXT PRIMARY KEY NOT NULL,
    "metadata" JSONB NOT NULL
    -- "extents": GEOMETRY
);

CREATE TABLE IF NOT EXISTS "jismesh_codes" (
    "code" bigint PRIMARY KEY NOT NULL,
    "level" integer NOT NULL,
    "geom" GEOMETRY (Polygon, 4326) NOT NULL
);
CREATE INDEX IF NOT EXISTS "jismesh_codes_geom_idx" ON "jismesh_codes" USING GIST (geom);
