def aggregatePrices():

    from bson.code import Code
    from pymongo import MongoClient

    client = MongoClient()
    db = client.cs4411

    mapper = Code(  """
                    function() {
                        key = { date: this.date };
                        value = { prices: [ {name: this.name, net: (this.close - this.open)} ] };
                        emit(key, value);
                    }
                    """)

    reducer = Code( """
                    function(key, values) {
                        prices_list = { prices: [] };
                        for(var i in values) {
                            prices_list.prices = values[i].prices.concat(prices_list.prices);
                        }
                        return prices_list;
                    }
                    """)

    db.data.map_reduce(mapper, reducer, "prices")
