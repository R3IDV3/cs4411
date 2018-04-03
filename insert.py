def insertPrices(filepath):

    import csv
    import datetime
    from pymongo import MongoClient

    client = MongoClient(socketTimeoutMS = None, connectTimeoutMS = None, serverSelectionTimeoutMS = 999999999, waitQueueTimeoutMS = None)
    db = client.cs4411

    currency_name = filepath.replace("data/","").replace("_price.csv","")

    with open(filepath, 'rb') as f:

        reader = csv.DictReader(f)

        for row in reader:
            # extract date string
            date = row.pop('Date')
            # re-format the date
            date = datetime.datetime.strptime(date, "%b %d, %Y")

            # lowercase keys and replace spaces with underscores
            # float values with no commas and no missing values
            row = dict((k.strip().lower().replace(" ", "_"), float(v.strip().replace(",","").replace("-",""))) for k, v in row.iteritems() if v.strip().replace(",","").replace("-","") != "")
            # row = {k.strip().lower().replace(" ", "_"): float(v.strip().replace(",","").replace("-","")) for k, v in row.iteritems()}

            # merge the name and date fields with the fields in the row
            document = {'name': currency_name, 'date': date}
            document.update(row)

            db.data.insert(document)


def insertHeadlines():

    import csv
    import datetime
    from pymongo import MongoClient

    client = MongoClient(socketTimeoutMS = None, connectTimeoutMS = None, serverSelectionTimeoutMS = 999999999, waitQueueTimeoutMS = None)
    db = client.cs4411

    with open("data/worldnews/RedditNews.csv", 'rb') as f:

        reader = csv.DictReader(f)

        for row in reader:
            # re-format the date
            date = datetime.datetime.strptime(row['Date'], "%Y-%m-%d")
            document = {'date': date, 'headline': row['News']}
            db.headlines.insert(document)
