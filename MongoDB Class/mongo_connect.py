from pymongo import MongoClient

conn_string='mongodb+srv://dawarabhay:Smokeweed69420@mongocluster.2bzvrp1.mongodb.net/?retryWrites=true&w=majority&appName=mongocluster'

client=MongoClient(conn_string)

db=client['airbnb']

collection=db['reviews']

# #inserting data
# insert_data={
#     'name':'John',
#     'age':30,
#     'city':'New York'
# }

# #inserting data into the collection
# insert_data=collection.insert_one(insert_data)
# print(f'Document inserted with id: {insert_data.inserted_id}')

# query the collection
# find_query = {'property_type' : 'Apartment', 'room_type': 'Private room'}

# find_query = { '$or' : [ {'property_type' : 'House'} , {'property_type' : 'Apartment'} ] }
# find_query = { 'room_type': 'Private room', 
#                '$or' : [ {'property_type' : 'House'} , {'property_type' : 'Apartment'} ] 
            # }
# find_query = { 'accommodates': { '$gt' : 2} }


# grp1 = [
#     {
#         "$group": {
#             "_id": "$address.country",  # Field to group by
#             "avg_price": {"$avg": "$price"}  # Field to avg
#         }
#     },
#     {
#         "$project": {
#             "country": "$_id",
#             "country_wise_avg_price": {"$toDouble": "$avg_price"},
#             "_id": 0
#         }
#     }
# ]

grp2 = [
    {
        "$group": {
            "_id": {
                "country": "$address.country",
                "city": "$address.suburb"
            },
            "avg_price": {"$avg": "$price"}
        }
    },
    {
        "$project": {
            "country": "$_id.country",
            "city": "$_id.city",
            "city_wise_avg_price": {"$toDouble": "$avg_price"},
            "_id": 0
        }
    }
]



# count_results = collection.count_documents(grp1)
# print("Total Documents Found : ",count_results)
# print("===========================")

#Printing the documents fetched from the collection
# find_results = collection.aggregate(grp1)
# for doc in find_results:
#     print(doc)
#     break

results = collection.aggregate(grp2)
for result in results:
    print(result)


#closing the connection
client.close()