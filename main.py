from pymongo import MongoClient

from insert import *
from mapreduce import *

client = MongoClient()
db = client.cs4411

print "Dropping the `data` database..."
db.data.drop()

print "Inserting data into `data` collection..."
insertPrices("data/bitcoin_price.csv")
insertPrices("data/bitconnect_price.csv")
insertPrices("data/dash_price.csv")
insertPrices("data/ethereum_classic_price.csv")
insertPrices("data/ethereum_price.csv")
insertPrices("data/iota_price.csv")
insertPrices("data/litecoin_price.csv")
insertPrices("data/monero_price.csv")
insertPrices("data/nem_price.csv")
insertPrices("data/neo_price.csv")
insertPrices("data/numeraire_price.csv")
insertPrices("data/omisego_price.csv")
insertPrices("data/qtum_price.csv")
insertPrices("data/ripple_price.csv")
insertPrices("data/stratis_price.csv")
insertPrices("data/waves_price.csv")
print "Done!"

print "Performing Map-Reduce to aggregate prices for each date..."
aggregatePrices()
print "Done!"

# Insert headlines
# Perform Map-Reduce on headlines to get keyword counts for each date (with stopwords removed)
# Join keywords with prices collection using common date
