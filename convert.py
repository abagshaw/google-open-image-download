from __future__ import unicode_literals
import csv
import six
import sys

def unicode_dict_reader(f, **kwargs):
    csv_reader = csv.DictReader(f, **kwargs)
    for row in csv_reader:
        yield {key: value for key, value in six.iteritems(row)}

def writeAnnotations(id, annotations):
  outFile = open("{}/{}.txt".format(sys.argv[2], id), 'w')
  outFile.write(annotations)
  outFile.close()

currentBoxes = None
prevImage = None

with open(sys.argv[1]) as f:
    for row in unicode_dict_reader(f):
        if row['ImageID'] != prevImage:
          if prevImage:
            writeAnnotations(prevImage, currentBoxes)
          prevImage = row['ImageID']
          currentBoxes = ""
        currentBoxes += "{} {} {} {} {}\n".format(row['LabelName'], row['XMin'], row['YMin'], float(row['XMax']) - float(row['XMin']), float(row['YMax']) - float(row['YMin']))
    
    #Catch trailing image
    if prevImage:
        writeAnnotations(prevImage, currentBoxes)