# python_harvesting
Set of Python scripts to harvest metadata into Esri Geoportal Server

# Scripts:

## generate_layer_files

This script crawls a local or network drive and all sub-folders and will try to create metadata and an ArcGIS Pro layer file for every valid dataset it encounters in all valid workspaces (including file and enterprise geodatabases). This script requires the ArcGIS Pro project and toolbox to run.