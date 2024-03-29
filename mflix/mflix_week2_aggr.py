from pymongo import MongoClient
import pprint

client = MongoClient('mongodb+srv://analytics:analytics-password@mflix.gygsu.mongodb.net/mflix?retryWrites=true&w=majority')
mflix = client.mflix
moviesInitial = mflix.movies_initial

# clean data and put it in movies_intial_cleaned
project_pipeline =[
    {
      '$addFields': {
          'lastupdated': {
              '$arrayElemAt': [
                  {'$split': ['$lastupdated', '.']},
                  0
                ]
          }
        }
    },
    {
        '$project': {
            'title': 1,
            'year': 1,
            'directors': {'$split': ['$director', ', ']},
            'actors': {'$split':  ['$actor', ', ']},
            'writers': {'$split': ['$writer', ', ']},
            'genres': {'$split': ['$genre', ', ']},
            'languages': {'$split': ['$language', ', ']},
            'countries': {'$split': ['$country', ', ']},
            'plot': 1,
            'fullPlot': '$fullplot',
            'rated': '$rating',
            'released': {
                '$cond': {
                    'if': {'$ne': ['$released', '']},
                    'then': {
                        '$dateFromString': {
                            'dateString': '$released'
                        }
                    },
                    'else': ''
                }
            },
            'runtime': 1,
            'imdb': {
                'id': '$imdbID',
                'rating': '$imdbRating',
                'votes': '$imdbVotes',
            },
            'metacritic': 1,
            'awards': 1,
            'type': 1,
            'lastUpdated': {
                '$cond': {
                    'if': {'ne': ['$lastupdated', '']},
                    'then': {
                        '$dateFromString': {
                            'dateString': '$lastupdated',
                            'timezone': 'America/New_York'
                        }
                    },
                    'else': ''
                }
            },
        },
    },
    {
        '$out': 'movies_initial_cleaned'
    }
]

# Execute aggr. function
moviesInitial.aggregate(project_pipeline)
