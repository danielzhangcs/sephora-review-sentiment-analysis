import os
import bs4
import requests
from bs4 import BeautifulSoup
import time
import json
import csv

global_count = 0
fileIndex = 1

def mkdir(filedir):
    if not os.path.exists(filedir):
        os.makedirs(filedir)

def wetherContainDS_Store(list):
    if ".DS_Store" in list:
        list.remove(".DS_Store")
    return list

def sortList(list):
    return sorted(list, key=lambda x: int(x))

# "prductID" vs index 0, "userName" vs index 1, "rating" vs index 2, "review" vs index 3
# "productID" == 0, "userNickname" == 1, "rating" == 2, "age" == 3, "eyeColor" == 4, 
# "hairColor" == 5, "skinTone" == 6, "skinType" == 7, "review" == 8
def writeToCSV(content, writer):
    global global_count
    content = content.split("\t");
    productID = content[0]
    userNickname = content[1]
    rating = content[2]
    age = content[3]
    eyeColor = content[4]
    hairColor = content[5]
    skinTone = content[6]
    skinType = content[7]
    review = content[8]
    print("======")
    writer.writerow([global_count, productID, userNickname, rating, age, eyeColor, hairColor, skinTone, skinType, review])

# This is the accese path of the data
filedir = "Sephora/Sephora_data_reviews"
dirList = [name for name in os.listdir(filedir)]
dirList = sortList(wetherContainDS_Store(dirList))
outputFileDir = "Sephora_data_reviews_csv"
mkdir(outputFileDir)
headerList = ["fileIndex", "productID", "userNickname", "rating", "age", "eyeColor", "hairColor", "skinTone", "skinType", "review"]
# headerList = ["fileIndex", "prductID", "userNickname", "rating", "review"]

csvFile = open(os.path.join(outputFileDir, "Sephora_data_part" + str(fileIndex) + ".csv"), "a+")
writer = csv.writer(csvFile)
writer.writerow(headerList)

for dirName in dirList:
    # print(dirName)
    filePath = os.path.join(filedir, dirName)
    for filename in sorted(os.listdir(filePath), key=lambda x: int(os.path.splitext(x)[0])):
        # print(filename)
        if global_count % 500000 == 0 and not global_count == 0:
            fileIndex += 1
            csvFile.close()
            csvFile = open(os.path.join(outputFileDir, "Sephora_data_part" + str(fileIndex) + ".csv"), "a+")
            writer = csv.writer(csvFile)
            writer.writerow(headerList)
        with open(os.path.join(filePath, filename), "r") as reviewFile:
            content = reviewFile.read();
            # print(content)
            writeToCSV(content, writer)
        print("global_count", global_count)
        global_count += 1
        # time.sleep(1)
csvFile.close()
    
