from pymongo import MongoClient, UpdateOne
import pprint
import re
from datetime import datetime

client = MongoClient('mongodb+srv://analytics:analytics-password@mflix.gygsu.mongodb.net/mflix?retryWrites=true&w=majority')
mflix = client.mflix

moviesInitial = mflix.movies_initial
moviesInitialCleaned = mflix.movies_initial_cleaned
movies = mflix.movies

# copy_pipeline = [
#     {
#         '$out': 'movies'
#     }
# ]
#
# moviesInitial.aggregate(copy_pipeline)
#
# print("Movies_intial copied to movies")

runtime_pat = re.compile(r'([0-9]+) min')

# for bulk update
batchSize = 1000
count = 0
updates = []

for movie in movies.find().limit(2000):
    fields_to_set = {}
    fields_to_unset = {}

    for key, value in movie.copy().items():
        if value == "" or value == [""]:
            del movie[key]
            fields_to_unset[key] = ""

    if 'director' in movie:
        fields_to_unset['director'] = ""
        fields_to_set['directors'] = movie['director'].split(", ")
    if 'cast' in movie:
        fields_to_set['cast'] = movie['cast'].split(", ")
    if 'writer' in movie:
        fields_to_unset['writer'] = ""
        fields_to_set['writers'] = movie['writer'].split(", ")
    if 'genre' in movie:
        fields_to_unset['genre'] = ""
        fields_to_set['genres'] = movie['genre'].split(", ")
    if 'language' in movie:
        fields_to_unset['language'] = ""
        fields_to_set['languages'] = movie['language'].split(", ")
    if 'country' in movie:
        fields_to_unset['country'] = ""
        fields_to_set['countries'] = movie['country'].split(", ")

    if 'fullplot' in movie:
        fields_to_unset['fullplot'] = ""
        fields_to_set['fullPlot'] = movie['fullplot']
    if 'rating' in movie:
        fields_to_unset['rating'] = ""
        fields_to_set['rated'] = movie['rating']

    imdb = {}
    if 'imdbID' in movie:
        fields_to_unset['imdbID'] = ""
        imdb['id'] = movie['imdbID']
    if 'imdbRating' in movie:
        fields_to_unset['imdbRating'] = ""
        imdb['rating'] = movie['imdbRating']
    if 'imdbVotes' in movie:
        fields_to_unset['imdbVotes'] = ""
        imdb['votes'] = movie['imdbVotes']
    if imdb:
        fields_to_set['imdb'] = imdb


    if 'released' in movie:
        fields_to_set['released'] = datetime.strptime(movie['released'], "%Y-%m-%d")

    if 'lastUpdated' in movie:
        fields_to_set['lastUpdated'] = datetime.strptime(movie['lastUpdated'][0:19], "%Y-%m-%d %H:%M:%S")

    if 'runtime' in movie:
        m = runtime_pat.match(movie['runtime'])
        if m:
            fields_to_set['runtime'] = int(m.group(1))

    update_doc = {}

    if fields_to_set:
        update_doc['$set'] = fields_to_set
    if fields_to_unset:
        update_doc['$unset'] = fields_to_unset

    # Update one
    # movies.update_one({'_id': movie['_id']}, update_doc)

    #for bulk updates, update every batch
    updates.append(UpdateOne({'_id' : movie['_id']}, update_doc))
    count += 1

    if count == batchSize:
        movies.bulk_write(updates)
        print("Batch updated")
        updates = []
        count = 0

if updates:
    movies.bulk_write(updates)
