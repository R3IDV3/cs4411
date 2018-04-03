import argparse
from pymongo import MongoClient
import bson
from insert import *
from mapreduce import *
from lookup import *
from unwinder import *
import time

def main():
    parser = argparse.ArgumentParser(description='Sentiment analysis using news headlines and cryptocurrency stock prices.')
    parser.add_argument('--data', action='store_true', help='Drop `data` collection and re-insert')
    parser.add_argument('--headlines', action='store_true', help='Drop `headlines` collection and re-insert')

    args = parser.parse_args()

    client = MongoClient(socketTimeoutMS = None, connectTimeoutMS = None, serverSelectionTimeoutMS = 999999999, waitQueueTimeoutMS = None)
    db = client.cs4411

    if args.data:
        print "Dropping the `data` collection..."
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

        print "Performing Map-Reduce to aggregate prices for each date into the `performance` collection..."
        aggregatePrices()
        print "Done!"

    if args.headlines:
        print "Dropping the `headlines` collection..."
        db.headlines.drop()
        print "Inserting data into `headlines` collection..."
        insertHeadlines()
        print "Done!"

        print "Performing Map-Reduce to store keywords from headlines in the `keywords` collection..."
        headlineKeywords()
        print "Done!"

        print "Count keywords..."
        countKeywords()
        print "Done!"

        # post-processing
        db.keywords.aggregate([
            {
                "$project": {
                    "_id": 0,
                    "date": "$_id.date",
                    "keyword": "$_id.keyword",
                    "count": "$value"
                }
            },
            {"$out": "keywords"}
        ])

    # Join keywords with prices collection using common date
    joinPerformanceKeywords()

    # print "unwindPerformance"
    unwindPerformance()
    # print "done"
    unwindPerformanceKeywords()
    # print "sleeping"
    # time.sleep(10)

    # print "unwound_performance_keywords"
    unwindPerformanceKeywords()
    # print "done"

    print "computing scores"
    keywordScore()
    print "done"

    # Perform Map-Reduce on headlines to get keyword counts for each date

if __name__ == '__main__':
    main()
