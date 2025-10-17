#!/usr/bin/env bash
set -euo pipefail
psql "$POSTGRES_URL" -f scripts/ingest_staging.sql
# python -m hhml.ingest.pp_xml <SIMD*.xml>
# python -m hhml.ingest.chart_xml <*tch.xml>
# psql "$POSTGRES_URL" -f scripts/merge_from_staging.sql
echo "Scaffold installed. Fill in parser code and run."
