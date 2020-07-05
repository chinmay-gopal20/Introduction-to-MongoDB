from pymongo import MongoClient
import pprint
client = MongoClient('mongodb+srv://analytics:analytics-password@mflix.gygsu.mongodb.net/mflix?retryWrites=true&w=majority')
mflix = client.mflix
moviesInitial = mflix.movies_initial


#filters language exits and not empty ('')
pipeline = [
    {
        '$match': {'language': {'$exists': 1, '$ne': ''} }
    },
    {
        '$sortByCount': '$language'
    },
    {
        '$facet': {
            'top languages combinations': [{'$limit': 100}],
            'unusual combinations shared by': [
                {
                    '$skip': 100
                },
                {
                    '$bucketAuto': {
                        'groupBy': '$count',
                        'buckets': 5,
                        'output': {
                            'language combinations': {'$sum': 1}
                        }
                    }
                }
            ]
        }
    }
]

output = list(moviesInitial.aggregate(pipeline))

pprint.pprint(output)
# print(len(output))
