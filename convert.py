from __future__ import unicode_literals
import csv
import six
import sys

def unicode_dict_reader(f, **kwargs):
    csv_reader = csv.DictReader(f, **kwargs)
    for row in csv_reader:
        yield {key: value for key, value in six.iteritems(row)}

def writeAnnotations(id, annotations):
  outFile = open("{}/{}.txt".format(sys.argv[3], id), 'w')
  outFile.write(annotations)
  outFile.close()

labels = {}
currentBoxes = None
prevImage = None

with open(sys.argv[2]) as f:
  lines = f.read().splitlines()
  num = 0
  for line in lines:
      labels[line] = num
      num += 1

with open(sys.argv[1]) as f:
    for row in unicode_dict_reader(f):
        if row['ImageID'] != prevImage:
          if prevImage:
            writeAnnotations(prevImage, currentBoxes)
          prevImage = row['ImageID']
          currentBoxes = ""
        
        if row['LabelName'] in labels:
          if 'XMin' not in row:
            print(row)
          currentBoxes += "{} {} {} {} {}\n".format(labels[row['LabelName']], (float(row['XMin']) + float(row['XMax'])) / 2, (float(row['YMin']) + float(row['YMax'])) / 2, float(row['XMax']) - float(row['XMin']), float(row['YMax']) - float(row['YMin']))
    
    #Catch trailing image
    if prevImage:
        writeAnnotations(prevImage, currentBoxes)