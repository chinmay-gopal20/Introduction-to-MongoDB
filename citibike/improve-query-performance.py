import pymongo
import pprint

# Replace XXXX with your connection URI from the Atlas UI
free_tier_client = pymongo.MongoClient('mongodb+srv://analytics:analytics-password@mflix.gygsu.mongodb.net/'
                                       'mflix?retryWrites=true&w=majority')

# We're using the people-raw dataset from the Cleansing Data with Updates assessment
people = free_tier_client.cleansing["people-raw"]

# This is a helper function to reduce the output of explain to a few key metrics
def distilled_explain(explain_output):
    return {
        'executionTimeMillis': explain_output['executionStats']['executionTimeMillis'],
        'totalDocsExamined'  : explain_output['executionStats']['totalDocsExamined'],
        'nReturned'          : explain_output['executionStats']['nReturned']
    }

query_1_stats = people.find({
  "address.state": "Nebraska",
  "last_name": "Miller",
}).explain()

query_2_stats = people.find({
  "first_name": "Harry",
  "last_name": "Reed"
}).explain()

# This is to provide a baseline for how long it takes to execute these queries
print(distilled_explain(query_1_stats))
print(distilled_explain(query_2_stats))

# pprint.pprint(query_1_stats)

# Replace "YYYY" with the best index to increase the performance of the two queries above
people.create_index('last_name')

query_1_stats = people.find({
  "address.state": "Nebraska",
  "last_name": "Miller",
}).explain()

query_2_stats = people.find({
  "first_name": "Harry",
  "last_name": "Reed"
}).explain()

# If everything went well, both queries should now have *much* lower execution times and documents examined
print(distilled_explain(query_1_stats))
print(distilled_explain(query_2_stats))

# pprint.pprint(query_1_stats)
