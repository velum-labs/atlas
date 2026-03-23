"""Pipeline package for Alma Atlas.

The pipeline orchestrates the scan → stitch → store workflow:
    1. ``alma_atlas.pipeline.scan``   — drive source adapters, collect raw data
    2. ``alma_atlas.pipeline.stitch`` — derive edges and consumers from raw data
"""
