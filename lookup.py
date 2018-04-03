def joinPerformanceKeywords():

    from pymongo import MongoClient

    client = MongoClient(socketTimeoutMS = None, connectTimeoutMS = None, serverSelectionTimeoutMS = 999999999, waitQueueTimeoutMS = None)
    db = client.cs4411

    db.performance.aggregate([
        {
            "$lookup": {
                "from": "keywords",
                "localField": "date",
                "foreignField": "date",
                "as": "keywords"
            }
        },
        {"$out": "performance_keywords"}
    ])
