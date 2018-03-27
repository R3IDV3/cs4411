# we use this structure for aggregating all the data (see proposal.pdf)
d = {
    "2017-03-04": {
        "date": "2017-03-04",
        "headlines": ["", ""],
        "prices": [
            {},
            {}
        ]
    },
    "2018-10-04": {
        "date": "2018-10-04",
        "headlines": ["", ""],
        "prices": [
            {},
            {}
        ]
    }
}

# we want this for inserting into mongodb
[
    {
        "date": "2017-03-04",
        "headlines": ["", ""],
        "prices": [
            {},
            {}
        ]
    },
    {
        "date": "2018-10-04",
        "headlines": ["", ""],
        "prices": [
            {},
            {}
        ]
    }
]

d["2017-03-04"]["prices"].append({"name": "bitcoin", "open": 123412})
d.values()
