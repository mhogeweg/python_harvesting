#!/usr/bin/python
#
# See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# Esri Inc. licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
 
# crawls a folder structure of xML files and publishes those to Geoportal Sever 2.x
# extended to also crawl into file geodatabases

import os
import sys
import requests
from requests.auth import HTTPBasicAuth
import arcpy
import arcpy.da
import uuid
import tempfile
import shutil
import urllib
from string import Template
from datetime import datetime
import json
from pathlib import Path
from xml.etree import ElementTree
import re
from xml.etree import ElementTree as et


# The URL for the geoportal 2.x's document management API.
server = 'https://www.example.com/geoportal/rest/metadata/item'
auth = HTTPBasicAuth('<username>', '<password>')
headers = {'Content-type': 'application/json'}

# setup working ArcGIS Pro project
aprx_base = r"work.aprx"
tmp_aprx = tempfile.gettempdir() + str(uuid.uuid4()) + ".aprx"
shutil.copy(aprx_base, tmp_aprx)
aprx = arcpy.mp.ArcGISProject(tmp_aprx)
the_map = aprx.listMaps()[0]

start_dir = r"C:\example\input"             # the physical (local) top of the network data structure to be crawled
sink_folder = r"C:\example\lyrx"            # the physical (local) folder of the virtual directory of layer files
waf_base = "https://www.example.com/lyrx/"  # the top level URL for the virtual directory to the folder containing the layer files that will be included in the metadata
overwrite_lyrx = True                       # if False, existing layer files will be reused. if True, existing layer files are removed and new layer file is written

# the default CRS. If no CRS is found in the data then the default CRS will be assumed.
# see https://www.spatialreference.org/ref/?search=Tennessee&srtext=Search
default_src_wkid = 4326

# arcgis metadata template
arcgis_template = """<?xml version="1.0"?>
<metadata xml:lang="en">
    <Esri>
        <CreaDate>$creadate</CreaDate> <!-- 20210520 -->
        <CreaTime>$creatime</CreaTime>  <!-- 10170200 -->
        <ArcGISFormat>1.0</ArcGISFormat>
        <SyncOnce>TRUE</SyncOnce>
        <ArcGISProfile>FGDC</ArcGISProfile>
    </Esri>
    <mdFileID>$mdFileID</mdFileID>
    <dataIdInfo>
        <idCitation xmlns="">
            <resTitle>$resTitle</resTitle>
            <date>
                <pubDate>$pubDate</pubDate> <!-- 2021-05-20T00:00:00 -->
            </date>
            <citRespParty xmlns="">
                <rpIndName>$rpIndName</rpIndName>
                <rpOrgName></rpOrgName>
                <role>
                    <RoleCd value="006"/>
                </role>
            </citRespParty>
        </idCitation>
        <searchKeys>$searchKeys</searchKeys> <!-- <keyword>keyword</keyword> -->
        <idPurp>$idPurp</idPurp>
        <idAbs>$idAbs</idAbs>
        <idCredit></idCredit>
        <resConst>
            <Consts>
                <useLimit></useLimit>
            </Consts>
        </resConst>
        <dataExt xmlns="">
            <geoEle xmlns="">
                <GeoBndBox esriExtentType="search">
                    <westBL>$westBL</westBL>
                    <eastBL>$eastBL</eastBL>
                    <northBL>$northBL</northBL>
                    <southBL>$southBL</southBL>
                    <exTypeCode>1</exTypeCode>
                </GeoBndBox>
            </geoEle>
            <exDesc></exDesc>
            <tempEle>
                <TempExtent>
                    <exTemp>
                        <TM_Period xmlns="">
                            <tmBegin>$tmBegin</tmBegin> <!-- 2021-02-01T00:00:00 -->
                        </TM_Period>
                    </exTemp>
                </TempExtent>
            </tempEle>
        </dataExt>
        <dataChar>
            <CharSetCd value="004"/>
        </dataChar>
        <spatRpType>
            <SpatRepTypCd value=""/>
        </spatRpType>
        <idStatus>
            <ProgCd value=""/>
        </idStatus>
        <resMaint xmlns="">
            <maintFreq>
                <MaintFreqCd value=""/>
            </maintFreq>
        </resMaint>
        <tpCat>
            <TopicCatCd value=""/>
        </tpCat>
    </dataIdInfo>
    <mdHrLv>
        <ScopeCd value=""/>
    </mdHrLv>
    <mdDateSt Sync="TRUE">$mdDateSt</mdDateSt>
    <mdContact xmlns="">
        <rpIndName>$rpIndName</rpIndName>
        <rpOrgName></rpOrgName>
        <role>
            <RoleCd value="006"/>
        </role>
        <displayName>$rpIndName</displayName>
        <rpCntInfo xmlns="">
            <cntAddress addressType="both">
                <city>$city</city>
                <adminArea>$adminArea</adminArea>
                <postCode>$postCode</postCode>
                <country>$country</country>
            </cntAddress>
            <cntPhone>
                <voiceNum tddtty="">$voiceNum</voiceNum>
            </cntPhone>
        </rpCntInfo>
    </mdContact>
    <dqInfo xmlns="">
        <dataLineage>
            <statement></statement>
            <dataSource xmlns="" type="">
                <srcDesc></srcDesc>
                <srcMedName>
                    <MedNameCd value="015"/>
                </srcMedName>
                <srcCitatn xmlns="">
                    <resTitle>Network File Path</resTitle>
                    <citOnlineRes xmlns="">
                        <linkage>$hierarchy</linkage>
                    </citOnlineRes>
                </srcCitatn>
            </dataSource>
        </dataLineage>
        <dqScope>
            <scpLvl>
                <ScopeCd value="005"/>
            </scpLvl>
        </dqScope>
    </dqInfo>
    <distInfo xmlns="">
        <distTranOps xmlns="">
            <onLineSrc xmlns="">
                <linkage>$linkage</linkage>
                <orFunct>
                    <OnFunctCd value="001"/>
                </orFunct>
            </onLineSrc>
            <onLineSrc xmlns="">
                <linkage>$file_link</linkage>
                <orFunct>
                    <OnFunctCd value="001"/>
                </orFunct>
            </onLineSrc>
        </distTranOps>
    </distInfo>
</metadata>"""  # template metadata in ArcGIS XML format
metadata_template = Template(arcgis_template)  # setup metadata template

log = sys.stdout  # Log stream


def get_projected_extent(e, in_wkid, wkid=4326):
    # e = get the extent
    # in_wkid = well-known id of the source extent
    # wkid = well-known id of output coordinate reference system (default to 4326)

    in_sr = arcpy.SpatialReference(in_wkid)
    out_sr = arcpy.SpatialReference(wkid)

    # convert the extent into a geometry
    # either as polygon of all 4 points or as a diagonal polyline
    e_points = [
        arcpy.Point(e.XMin, e.YMin),
        arcpy.Point(e.XMax, e.YMax),
        ]
    e_geometry = arcpy.Polyline(arcpy.Array(e_points), in_sr)

    # project the geometry and get the projected extent
    e_proj = e_geometry.projectAs(out_sr).extent

    return e_proj


def publish_metadata(metadata):
    # use HTTP PUT on the Geoportal Server API to add this metadata to the catalog

    r = requests.put(url=server, data=metadata, auth=auth, headers=headers)
    print(f"{r.status_code} - {r.text}\n'")


def get_hierarchy_from_file(f):
    # returns the folder hierarchy from file f
    # example:
    #   C:/data/arcgis/USA\USFS_AdministrativeRegion.mxd
    # results in:
    #   C|data|arcgis|USA|USFS_AdministrativeRegion.mxd

    hierarchy = os.path.dirname(f).split(start_dir)[-1].replace("\\\\", "\\").replace("\\", "|").replace("/", "|")[1:]
    # print(f"hierarchy = {hierarchy}")
    return hierarchy


def combine_element(f_xml, t_xml):
    # This function recursively updates either the text or the children
    # of an element if another element is found in `one`, or adds it
    # from `other` if not found.

    # Create a mapping from tag name to element, as that's what we are fltering with
    mapping = {el.tag: el for el in f_xml}
    for el in t_xml:
        if len(el) == 0:
            # Not nested
            try:
                # Update the text
                mapping[el.tag].text = el.text
            except KeyError:
                # An element with this name is not in the mapping
                mapping[el.tag] = el
                # Add it
                f_xml.append(el)
        else:
            try:
                # Recursively process the element, and update it in the same way
                combine_element(mapping[el.tag], el)
            except KeyError:
                # Not in the mapping
                mapping[el.tag] = el
                # Just add it
                f_xml.append(el)


def merge_metadata(f_metadata, t_metadata):
    # merges existing metadata for datasets with what was extracted by this script
    # removes FGDC metadata elements, since we're publishing ArcGIS XML metadata

    merged_metadata = ""

    f_xml = ElementTree.fromstring(f_metadata)  # geodatabase metadata
    t_xml = ElementTree.fromstring(t_metadata)  # template-based metadata

    # merge the metadata from the geodatabase and the template
    f_xml.extend(t_xml)
    merged_metadata = ElementTree.tostring(f_xml).decode('UTF-8').replace("\\", "/").replace("\n", "")

    # stripping FGDC metadata elements
    merged_metadata = re.sub(r"<idinfo>.*</idinfo>", " ", merged_metadata)
    merged_metadata = re.sub(r"<dataqual>.*</dataqual>", " ", merged_metadata)
    merged_metadata = re.sub(r"<spdoinfo>.*</spdoinfo>", " ", merged_metadata)
    merged_metadata = re.sub(r"<spref>.*</spref>", " ", merged_metadata)
    merged_metadata = re.sub(r"<eainfo>.*</eainfo>", " ", merged_metadata)
    merged_metadata = re.sub(r"<distinfo>.*</distinfo>", " ", merged_metadata)
    merged_metadata = re.sub(r"<metainfo>.*</metainfo>", " ", merged_metadata)
    merged_metadata = re.sub(r"<smusrdef>.*</smusrdef>", " ", merged_metadata)

    print(f"merged_metadata = {merged_metadata}")

    return merged_metadata


def generate_layer_file(folder, data_file):
    # generates an ArcGIS Pro layer file (.lyrx) for the dataset.
    # the dataset is added to an empty ArcGIS Pro project file.
    # this function then generates the path to the layer file as it will be
    # referenced in the metadata. It exports the layer file to the current
    # workspace. For some datasets, the reference to the dataset needs to be
    # updated as the layer file in the sink folder (the one that should have a URL
    # and is referenced in the metadata) is not in the same location as the
    # dataset itself. Finally, the layer of the data_file is removed from
    # the ArcGIS Pro project file

    f = os.path.join(folder, data_file)
    desc = arcpy.Describe(f)
    print(f"Parsing = {data_file}")

    # generate the layer file
    try:
        layer = the_map.addDataFromPath(f)
    except RuntimeError as e:
        print(f"RuntimeError: {e}")
        return ""

    dataset_file_name = os.path.join(folder, data_file)
    lyr_file_name = os.path.splitext(dataset_file_name)[0].replace('.sde/', '_sde/').replace('.gdb/', '_gdb/').replace('\\', '/') \
                    + '_' + os.path.splitext(dataset_file_name)[-1].replace('.', '') + '.lyrx'
    print(f"dataset_file_name = {dataset_file_name}")

    # if the workspace is a file geodatabase or enterprise geodatabase, the layer file
    # cannot be stored inside the workspace itself. In this case, create a folder
    # with name similar to the geodatabase name to put the lyrx in.
    if any(word in lyr_file_name for word in [".sde", ".gdb"]):
        # If the folder doesn't exist yet, create it here
        gdb_lyrx_folder = lyr_file_name.rsplit('.', 2)[0] + ".gdb_layers"
        print(f"creating folder {gdb_lyrx_folder}")
        if not os.path.exists(gdb_lyrx_folder):
            Path(gdb_lyrx_folder).mkdir(parents=True, exist_ok=True)

        lyr_file_name = os.path.join(gdb_lyrx_folder, data_file) + '_fc.lyrx'

    print(f"lyr_file_name     = {lyr_file_name}")

    # if the layer file already existed, remove it now if so set
    lyr_file = ""
    if os.path.exists(lyr_file_name):
        if overwrite_lyrx:
            # remove existing layer file and save dataset to a layer file locally with the data
            os.remove(lyr_file_name)
            lyr_file = layer.saveACopy(lyr_file_name)
        else:
            lyr_file = lyr_file_name
    else:
        # save to a new layer file locally with the data
        lyr_file = layer.saveACopy(lyr_file_name)

    print(f"lyr_file = {lyr_file_name}")

    # now save a copy of the layer file to the web-accessible folder
    # overwriting a pre-existing version of the layer file again
    dataset_download_name = os.path.join(sink_folder, os.path.basename(lyr_file))
    # print(f"lyr_download_name = {dataset_download_name}")
    lyr_download_name = os.path.splitext(dataset_download_name)[0] + '.lyrx'
    if os.path.exists(lyr_download_name):
        os.remove(lyr_download_name)
    lyr_file_copy = layer.saveACopy(dataset_download_name)

    # fix data source to be absolute for the download version
    connection_string = ""
    lyr_json = {}
    with open(lyr_file_copy) as lyrx_json_file:
        lyr_json = json.load(lyrx_json_file)
        print(f"type = {desc.dataType}")

        if desc.dataType in ["ShapeFile"]:
            lyr_json["layerDefinitions"][0]["featureTable"]["dataConnection"]["workspaceConnectionString"] = f"DATABASE={folder}"
        elif desc.dataType in ["RasterDataset", "LasDataset"]:
            lyr_json["layerDefinitions"][0]["dataConnection"]["workspaceConnectionString"] = f"DATABASE={folder}"
        elif desc.dataType in ["FeatureClass"]:
            print(f"desc = {desc}")
        else:
            print(f"ERROR - Unknown Data Type: {desc.dataType}")

    with open(lyr_file_copy, 'w') as lyrx_json_file:
        json.dump(lyr_json, lyrx_json_file)

    the_map.removeLayer(layer)

    # metadata = get_metadata(f, desc, lyr_file_copy, lyr_download_name)
    return f, desc, lyr_file_copy, lyr_download_name


def generate_metadata(f, desc, lyr_file_copy, lyr_download_name):
    # generate metadata for the dataset f, but use lyr_file_copy as the link in the metadata

    file_path = os.path.basename(lyr_file_copy)
    file_path_no_drive = file_path.split(":")[-1]
    link = waf_base + f"{file_path_no_drive}"
    file_link = f"file://{f}"
    # print(f"link = {link}")

    now = datetime.now()
    this_day_time = now.strftime("%Y-%m-%dT%H:%M:%S")
    this_day = now.strftime("%Y%m%d")
    this_time = now.strftime("%H%M%S")

    # store hierarchy and data type as keywords
    hierarchy = get_hierarchy_from_file(f)
    search_keys = ''  # '<keyword>' + hierarchy + '</keyword>'
    search_keys += '<keyword>' + desc.dataType + '</keyword>'

    # get the title and clean it from unallowed characters
    title = re.sub(r"[_\.\$]", " ", desc.baseName)

    # get the description. add LAS attributes here
    description = f"description {desc.baseName} is of type {desc.dataType}"
    if desc.dataType == "LasDataset":
        description += f". constraintCount = {desc.constraintCount}, fileCount = {desc.fileCount}, hasStatistics = {desc.hasStatistics}, needsUpdateStatistics = {desc.needsUpdateStatistics}, pointCount = {desc.pointCount}"

    # get extent. the default CRS needs to be set at the top of this script
    extent = desc.extent
    src_wkid = desc.SpatialReference.factoryCode
    if src_wkid is None:
        src_wkid = default_src_wkid
    elif src_wkid < 1:
        src_wkid = default_src_wkid
    print(f"Source wkid = {src_wkid}, converting to 4326 for boudning box")

    try:
        extent_4326 = get_projected_extent(extent, src_wkid, 4326)
        xmin = min(max(extent_4326.XMin if extent_4326.XMin is not None else -180, -180), 180)
        xmax = max(min(extent_4326.XMax if extent_4326.XMax is not None else 180, 180), -180)
        ymin = min(max(extent_4326.YMin if extent_4326.YMin is not None else -90, -90), 90)
        ymax = max(min(extent_4326.YMax if extent_4326.YMax is not None else 90, 90), -90)

    except:
        xmin = -180
        xmax = 180
        ymin = -90
        ymax = 90

    # fill content structure that will be put into the metadata template
    content = {
        'mdFileID': file_link,
        'creadate': this_day,
        'creatime': this_time,
        'resTitle': title,
        'pubDate': this_day,
        'rpIndName': 'Jenny',
        'identifier': link,
        'searchKeys': search_keys,
        'idPurp': '',
        'idAbs': description,
        'westBL': xmin,
        'eastBL': xmax,
        'northBL': ymax,
        'southBL': ymin,
        'tmBegin': this_day_time,
        'mdDateSt': this_day_time,
        'displayName': 'Jenny',
        'city': 'Example Town',
        'adminArea': '',
        'postCode': 00000,
        'country': 'US',
        'voiceNum': '(000) 867-5309',
        'hierarchy': hierarchy,
        'linkage': link,
        'file_link': file_link
    }
    # print(f"content => {content}")
    t_metadata = metadata_template.substitute(content)

    # if the data already has metadata (e.g. in geodatabase), fetch it
    # and merge the above templetized metadata with it
    metadata = t_metadata
    f_metadata = arcpy.metadata.Metadata(f)
    if f_metadata:
        # metadata = merge_metadata(f_metadata.xml, t_metadata)
        f_xml = ElementTree.fromstring(metadata)  # geodatabase metadata
        t_xml = ElementTree.fromstring(f_metadata.xml)  # template-based metadata

        combine_element(f_xml, t_xml)
    metadata = ElementTree.tostring(f_xml, encoding='unicode', method='xml')

    xml_download_name = lyr_download_name + '.xml'
    with open(xml_download_name, 'w') as xml_file:
        xml_file.write(metadata)
        print(f"xml_file => {xml_file}")

    return metadata


def parse_workspace(workspace):
    # parse a single folder as a workspace
    print(f"workspace = {workspace}")

    arcpy.env.workspace = workspace

    # make a list of ArcGIS compatible datasets
    # then create a layer file for the dataset
    # then create a metadata file for the layer file
    # then publish metadata of the layer file to the geoportal
    datasets = arcpy.ListFeatureClasses()
    for dataset in datasets:
        desc = arcpy.Describe(dataset)
        print(f"dataset.dataType = {desc.dataType}")

        f, desc, lyr_file_copy, lyr_download_name = generate_layer_file(workspace, dataset)
        metadata = generate_metadata(f, desc, lyr_file_copy, lyr_download_name)

        if len(metadata) > 0:
            publish_metadata(metadata)

    # if not inside a Feature Dataset
    # do the same for Feature Datasets and the Feature Classes they contain
    print(f"Getting feature classes")
    for feature_dataset in arcpy.ListDatasets('', 'feature'):
        desc = arcpy.Describe(feature_dataset)
        print(f"desc.dataType = {desc.dataType}")
        if desc.dataType == "FeatureDataset":
            gdb_workspace = arcpy.env.workspace
            arcpy.env.workspace = workspace + "/" + feature_dataset
            print(f"crawling feature dataset {feature_dataset}")
            parse_workspace(workspace + "/" + feature_dataset)
            arcpy.env.workspace = gdb_workspace

    print(f"Getting rasters")
    rasters = arcpy.ListRasters()
    for raster in rasters:
        if raster.endswith('.pmf'):
            print("ALERT: ArcReader files not supported")
            continue
        elif raster.endswith(".cej"):
            print("ALERT: CityEngine files not supported")
            continue
        elif raster.endswith(".ttf"):
            print("ALERT: Font files not supported")
            continue
        elif raster.endswith(".las"):
            print(f"LAS file {raster}!")

        f, desc, lyr_file_copy, lyr_download_name = generate_layer_file(workspace, raster)
        metadata = generate_metadata(f, desc, lyr_file_copy, lyr_download_name)
        if len(metadata) > 0:
            publish_metadata(metadata)

    print(f"Getting LAS files")
    lasses = [f for f in os.listdir(workspace) if re.match(r'.*\.las', f)]
    for las in lasses:
        print(f"LAS file {las}!")

        f, desc, lyr_file_copy, lyr_download_name = generate_layer_file(workspace, las)
        metadata = generate_metadata(f, desc, lyr_file_copy, lyr_download_name)
        if len(metadata) > 0:
            publish_metadata(metadata)


def main():
    # build list of folders recursively, skipping file geodatabase folders
    workspaces = [x[0] for x in os.walk(start_dir)]  # if not (x[0].endswith('.gdb') or ('_alllayers' in x[0]))]

    # use only start_dir by turning that into a 1-element list
    # workspaces = [start_dir]

    # crawl each of the folders as a workspace
    for workspace in workspaces:
        parse_workspace(workspace)


if __name__ == '__main__':
    main()
