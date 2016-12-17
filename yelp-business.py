#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Dec  9 15:23:24 2016

@author: Sriharish
"""

import os
import time
import json

from pymongo import MongoClient

from settings import Settings


dataset_file = Settings.DATASET_FILE
business_collection = MongoClient(Settings.MONGO_CONNECTION_STRING)[Settings.REVIEWS_DATABASE][
    Settings.BUSINESS_COLLECTION]

business_file = Settings.BUSINESS_FILE
#business_ids = []


count = 0
done = 0
start = time.time()

with open(business_file) as business:
#    next(business)
    for line in business:
        try:
            data = json.loads(line)
        except ValueError:
            print("Error in reading file")
            
        attributes = data["attributes"]
        try:
            temp = attributes["Take-out"]
        except KeyError:
            attributes["Take-out"] = ""
        try:
            temp = attributes["Drive-Thru"]
        except KeyError:
            attributes["Drive-Thru"] = ""
        try:
            temp = attributes["Delivery"]
        except KeyError:
            attributes["Delivery"] = ""
        try:
            temp = attributes["Alcohol"]
        except KeyError:
            attributes["Alcohol"] = ""
        try:
            temp = attributes["Price Range"]
        except KeyError:
            attributes["Price Range"] = ""
        try:
            temp = attributes["Parking"]
        except KeyError:
            attributes["Parking"] = ""
        
        categories = data["categories"]
        if (data["city"] == "Charlotte") and ("Restaurants" in categories):
            business_collection.insert_one({
                "business_id": data["business_id"],                                        
                "name": data["name"],
                "full_address": data["full_address"],
                "stars": data["stars"],
                "Take-out": attributes["Take-out"],
                "Drive-Thru": attributes["Drive-Thru"],
                "Delivery": attributes["Delivery"],
                "Alcohol": attributes["Alcohol"],
                "Price Range": attributes["Price Range"],
                "Parking": attributes["Parking"]
            })
        done += 1
        if done % 100 == 0:
            end = time.time()
            os.system('cls')
            print('Done ' + str(done) + ' in ' + str((end - start)))
