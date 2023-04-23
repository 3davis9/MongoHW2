#NOTE: Remove tables in Atlas before running this script!

import pymongo
import datetime, pprint
from pymongo import MongoClient

client = MongoClient("mongodb+srv://edavis17:Secret02--@clusterluc.llb0viv.mongodb.net/test")
db = client.sample_airbnb
collection = db.listingsAndReviews


#QUESTION 1
print('\n'+'### Question 1 ###')
query = {
  "$and": [
    { "$or": [{ "amenities": { "$all": ["Wide entryway", "Wide doorway"] } }] },
    { "$or": [{ "amenities": { "$all": ["Waterfront"] } }] }
  ]
}

projection = {
  "name":1,
  "beds":1
}

# Execute the query with sorting and limits
results = collection.find(query, projection).sort("beds", -1).limit(10)

# Print 
for doc in results:
    print(doc)



#QUESTION 2
print('\n'+'### Question 2 ###')
query1 = {}
projection = {
  "_id": 1,
  "name": 1,
  "price": { "$ifNull": [ "$price", 0 ] }
}

results = collection.find(query1, projection).sort("price", -1).limit(5)

for doc in results:
    print(doc)




#QUESTION 3
print('\n'+'### Question 3 ###')
query3 = {
    "$expr": { "$gt": [ "$review_scores_cleanliness", { "$ifNull": [ "$review_scores_location", 0 ] } ] }
}
projection = {
    "_id": 1,
    "name": 1,
    "bathrooms": 1,
    "review_scores_cleanliness": 1,
    "review_scores_location": 1
}

results = collection.find(query3, projection).sort("bathrooms", -1).limit(5)

for doc in results:
    print(doc)



#QUESTION 4
print('\n'+'### Question 4 ###')
query4 = { "address.country": "Portugal" }
projection = {
    "_id": 1,
    "name": 1,
    "address.country": 1,
    "number_of_reviews": 1
}
results = collection.find(query4, projection).sort("number_of_reviews", -1).limit(5)

for doc in results:
    print(doc)



#QUESTION 5
print('\n'+'### Question 5 ###')

query5 = {
    "number_of_reviews": { "$gt": 50 },
    "review_scores_cleanliness": 10
}
projection = {
    "_id": 1,
    "name": 1,
    "number_of_reviews": 1,
    "review_scores_cleanliness": 1
}
results = collection.find(query5, projection).sort("number_of_reviews", -1).limit(5)

# Print the results
for doc in results:
    print(doc)


#QUESTION 6
print('\n'+'### Question 6 ###')

pipeline = [
  {
    "$match": {
      "host_response_rate": { "$lt": "90%" }
    }
  },
  {
    "$group": {
      "_id": "$host_id",
      "host_name": { "$first": "$host_name" },
      "host_listings_count": { "$sum": 1 }
    }
  },
  {
    "$sort": {
      "host_listings_count": -1
    }
  },
  {
    "$limit": 5
  }
]

# Execute the aggregation query
results = db.listingsAndReviews.aggregate(pipeline)

# Print the results
for result in results:
  print("Host ID: ", result["_id"])
  print("Host Name: ", result["host_name"])
  print("Host Listings Count: ", result["host_listings_count"])
