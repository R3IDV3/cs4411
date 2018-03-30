def insertPrices(filepath):

    import csv
    import datetime
    from pymongo import MongoClient

    client = MongoClient()
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