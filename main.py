import argparse
from pymongo import MongoClient
from insert import *
from mapreduce import *

def main():
    parser = argparse.ArgumentParser(description='Sentiment analysis using news headlines and cryptocurrency stock prices.')
    parser.add_argument('--data', action='store_true', help='Drop `data` collection and re-insert')
    parser.add_argument('--headlines', action='store_true', help='Drop `headlines` collection and re-insert')

    args = parser.parse_args()

    client = MongoClient()
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

    # Perform Map-Reduce on headlines to get keyword counts for each date
    # Join keywords with prices collection using common date

if __name__ == '__main__':
    main()
