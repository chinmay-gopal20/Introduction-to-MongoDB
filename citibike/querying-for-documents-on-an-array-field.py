from pymongo import MongoClient
import pprint

# We're just reading data, so we can use the course cluster
client = MongoClient('mongodb://analytics-student:analytics-password@cluster0-shard-00-00-jxeqq.mongodb.net:27017,cluster0-shard-00-01-jxeqq.mongodb.net:27017,cluster0-shard-00-02-jxeqq.mongodb.net:27017/?ssl=true&replicaSet=Cluster0-shard-0&authSource=admin')

# We'll be using two different collections this time around
movies = client.mflix.movies
surveys = client.results.surveys

# Replace XXXX with a filter document that will find the movies where "Harrison Ford" is a member of the cast
movie_filter_doc = {'cast': {'$in': ['Harrison Ford']}}

# This is the first part of the answer to the lab
print(movies.find(movie_filter_doc).count())

# Replace YYYY with a filter document to find the survey results where the "abc" product scored greater than 6
survey_filter_doc = {'results':
                         {'$elemMatch':
                              {
                                  'product': 'abc',
                                  'score': {'$gt': 6}
                              }
                          }
                     }

# This is the second part of the answer to the lab
results = surveys.find(survey_filter_doc)

print(results.count())