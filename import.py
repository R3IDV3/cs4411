import csv
import json
import sys
import sys, getopt, pprint
from pymongo import MongoClient

#grab file location from command line
filepath = sys.argv[1]
csvfile = open(filepath, 'r')

# reader = csv.DictReader( csvfile )
mongo_client = MongoClient()
db = mongo_client.cs4411


# reader = csv.reader(infile)
reader = csv.DictReader( csvfile )


for each in reader:
    pDict = []
    pDict.append(each)
    database = {'date':pDict[0]['Date'], 'headlines':[], 'prices':pDict}
    db.data.insert(database)
