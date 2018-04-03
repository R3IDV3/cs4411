def unwindPerformance():

    from pymongo import MongoClient

    client = MongoClient(socketTimeoutMS = None, connectTimeoutMS = None, serverSelectionTimeoutMS = 999999999, waitQueueTimeoutMS = None)
    db = client.cs4411

    db.performance_keywords.aggregate([
        {
            "$unwind": {
                "path": "$performance"
            }
        },
        {
            "$project": {
                "_id": 0,
            }
        },
        {"$out": "unwound_performance"}
    ])

def unwindPerformanceKeywords():

    from pymongo import MongoClient

    client = MongoClient(socketTimeoutMS = None, connectTimeoutMS = None, serverSelectionTimeoutMS = 999999999, waitQueueTimeoutMS = None)
    db = client.cs4411

    # db.createCollection("unwound_performance_keywords", {
    #     "pipeline": [
    #         {
    #             "$unwind": {
    #                 "path": "$keywords"
    #             }
    #         },
    #         {
    #             "$project": {
    #                 "_id": 0,
    #             }
    #         }
    #     ]
    # })

    print "in unwinder"
    db.unwound_performance.aggregate([
        {
            "$unwind": {
                "path": "$keywords"
            }
        },
        {
            "$project": {
                "_id": 0,
            }
        },
        {"$out": "unwound_performance_keywords"}
    ])
