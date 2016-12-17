import os
import time
import json

from pymongo import MongoClient

from settings import Settings


dataset_file = Settings.DATASET_FILE
reviews_collection = MongoClient(Settings.MONGO_CONNECTION_STRING)[Settings.REVIEWS_DATABASE][
    Settings.REVIEWS_COLLECTION]

business_file = Settings.BUSINESS_FILE
business_ids = []
with open(business_file) as business:
    next(business)
    for line in business:
        try:
            data = json.loads(line)
        except ValueError:
            print("Error in reading file")
        if data["city"] == "Charlotte":
            business_ids.append(data["business_id"])
    
    
    
count = 0
done = 0
start = time.time()

with open(dataset_file) as dataset:
    count = sum(1 for line in dataset)

with open(dataset_file) as dataset:
    next(dataset)
    for line in dataset:
        try:
            data = json.loads(line)
        except ValueError:
            print('Oops!')
        if (data["type"] == "review") and (data["business_id"] in business_ids):
            reviews_collection.insert({
                "reviewId": data["review_id"],
                "business": data["business_id"],
                "stars": data["stars"],
                
                "text": data["text"]
            })

        done += 1
        if done % 100 == 0:
            end = time.time()
            os.system('cls')
            print('Done ' + str(done) + ' out of ' + str(count) + ' in ' + str((end - start)))
