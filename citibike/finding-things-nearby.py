import pymongo
import pprint

course_cluster_uri = 'mongodb://analytics-student:analytics-password@cluster0-shard-00-00-jxeqq.mongodb.net:27017,' \
                     'cluster0-shard-00-01-jxeqq.mongodb.net:27017,cluster0-shard-00-02-jxeqq.mongodb.net:27017/?' \
                     'ssl=true&replicaSet=Cluster0-shard-0&authSource=admin'

course_client = pymongo.MongoClient(course_cluster_uri)
trips = course_client.citibike.trips

# pprint.pprint(trips.find_one())

# trips.create_index('start station location')

# Replace XXXX with your query to find all trips that started .5 miles from 40.75741088433861, -73.98782093364191
query = {
    'start station loction': {
        '$nearSphere': {
            '$geometry': {
                'type': 'Point',
                'coordinates': [-73.98782093364191, 40.75741088433861]
            },
            '$minDistance': 0,
            '$maxDistance': 804.672
        }
    }
}

print(trips.find(query).count())