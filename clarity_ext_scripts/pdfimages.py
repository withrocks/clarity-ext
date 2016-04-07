import os
import requests
import string
import sys
import xml.etree.ElementTree as ET

# compoundOutputFileLuids3} {username} {password} {outputFileLuids}"
### BOILERPLATE, should be able to get this
### with only `context.compound_output_file[3]`
### which will download the file (lazily), if it's not available and then
### return the name of it
hostname = "lims-staging.snpseq.medsci.uu.se"
username = sys.argv[2]
password = sys.argv[3]
artifactLUID = sys.argv[1]

artif_URI = "http://" + hostname + ":8080/api/v2/artifacts/" + artifactLUID
artGET = requests.get(artif_URI, auth=(username, password))
artXML = artGET.text
root = ET.fromstring(artXML)
for child in root:
    if child.tag == "{http://genologics.com/ri/file}file":
        fileLUID = child.get('limsid')

file_URI = "http://" + hostname + ":8080/api/v2/files/" + fileLUID + "/download"
fileGET = requests.get(file_URI, auth=(username,password))      #GET request
with open("frag.pdf", 'wb') as fd:
    for chunk in fileGET.iter_content():                        #saving data stream to file in local
        fd.write(chunk)
tempwd = os.getcwd()
thePDF = tempwd + "/frag.pdf"           #temp PDF will be in this location

wells=[]
for i in range(1,13):
    for a in list(string.ascii_uppercase[:8]):
        well = a+str(i)
        wells.append(well)

for each in range(len(wells)):
    page = 10 + each                #first image is on page 10
    well_loci = wells[each]
    limsid = sys.argv[each + 4]
    filename = limsid + "_" + well_loci
    # TODO: PDF package doesn't exist
    command = 'pdfimages ' + thePDF +' -j -f ' + str(page) + ' -l ' + str(page) + ' ' + filename
    os.system(command)
    longname = filename + "-000"
    ppmname = longname + ".ppm"

    # TODO: convert command doesn't exist
    jpegname = longname + ".jpeg"
    command2 = "convert " + ppmname + " " + jpegname
    os.system(command2)

    # TODO: Don't allow this crap!
    command3 = "rm *ppm"            #removing ppm image so it isn't inadvertently attached
    os.system(command3)

print "Attaching electrophereagram image to each sample"
