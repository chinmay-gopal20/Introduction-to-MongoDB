from pymongo import MongoClient
import pprint

# the connection uri to our course cluster
client = MongoClient('mongodb://analytics-student:analytics-password@cluster0-shard-00-00-jxeqq.mongodb.net:27017,'
                     'cluster0-shard-00-01-jxeqq.mongodb.net:27017,'
                     'cluster0-shard-00-02-jxeqq.mongodb.net:27017/?ssl=true&replicaSet=Cluster0-shard-0&authSource=admin')

# the trips collection on the citibike database
trips = client.citibike.trips

#######################################################################################################################################

#find all trips between 5 and 10 minutes in duration that start at station 216.

# find all trips between 5 and 10 minutes in duration that start at station 216
query = {"tripduration":{"$gte":5000,"$lt":10000},"start station id":216}

# only return the bikeid, tripduration, and _id (displayed by default)
projection = {"bikeid": 1, "tripduration": 1}

print('find all trips between 5 and 10 minutes in duration that start at station 216.')

count=0
# print all of the trips
for doc in trips.find(query, projection):
    pprint.pprint(doc)
    count+=1

print("total number of trips : ", count)

#######################################################################################################################################

#find Citibike trips that start at station id 279 end most frequently at what station id?

match = {'$match': {'start station id': 279}}
sortByCount= {'$sortByCount': '$end station id'}

print('find Citibike trips that start at station id 279 end most frequently at what station id?')
pprint.pprint(list(trips.aggregate([match, sortByCount])))

