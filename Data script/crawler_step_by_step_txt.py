import os
import bs4
import requests
from bs4 import BeautifulSoup
import time
import json
import csv

LIMIT = 100
global_count = 0
fileIndex = 1

# get all the brands url
# and get all the brandID
def get_brands_list(url, filedir):
    res = requests.get(url)
    soup = BeautifulSoup(res.text, "html.parser")
    brand_urls = []
    brand_ids = []
    base_url = "https://www.sephora.com"
    count = 0

    csvfile = open(os.path.join(filedir, "Brands_URL.csv"), "a+")
    writer = csv.writer(csvfile)
    writer.writerow(["Brand_ID", "Brand_URL"])

    for href in soup.findAll("a", {"class": "u-hoverRed u-db u-p1"}):
        brand_url = base_url + href.get('href')
        print(brand_url)
        brandID = get_brandID(brand_url)
        print(count, "\t", brandID)

        writer.writerow([brandID, brand_url])

        brand_ids.append(brandID)
        brand_urls.append(brand_url)
        count += 1
    csvfile.close()
    return brand_urls, brand_ids


# get the brandID
def get_brandID(url):
    res = requests.get(url)
    soup = BeautifulSoup(res.text, "html.parser")
    brand_tag = soup.select("div[certona='BRAND']")
    # print(x)
    for item in brand_tag:
        brandID = item.get("data-brand_id")
        # print(brandID)
    # brandID = soup.select("div seph-search").get('data-brand-id')
    return brandID

# https://www.sephora.com/rest/products/?products=all&currentPage=2&include_categories=true&include_refinements=true&brandId=3902
# https://www.sephora.com/rest/products/?products=all&include_categories=true&include_refinements=true&brandId=5847
def get_all_productID(brand_ids, writer):
    all_productID = []
    for brand_id in brand_ids:
        product_ids_by_brand = get_productID_by_brand(writer, brand_id)
        # print(product_ids_by_brand, len(product_ids_by_brand))
        if len(product_ids_by_brand) != 0:
            all_productID.extend(product_ids_by_brand)
    return all_productID

def get_productID_by_brand(writer, brand_id, currentPage=1):
    url = "https://www.sephora.com/rest/products/?products=all&include_categories=true&include_refinements=true&brandId=" + str(brand_id) + "&currentPage=" + str(currentPage)
    data=requests.get(url)
    json_data = json.loads(data.content)

    pageSize = json_data["page_size"]
    totalProducts = json_data["total_products"]
    
    brand_name = json_data["brand"]["brand_name"]
    if not brand_name is not None:
        brand_name = "NA"

    if (int(totalProducts) == 0):
        return []


    # for eliminate the ERROR: list index out of range  
    if (len(json_data["products"]) < int(totalProducts)):
        threshold = len(json_data["products"])
    else:
        threshold = json_data["total_products"]
    print(threshold)
    
    product_ids_by_brand = []
    for index in range(threshold):
        productID = json_data["products"][index]["id"]
        
        product_name = json_data["products"][index]["display_name"]
        if not product_name is not None:
            product_name = "NA"
        writer.writerow([productID, product_name, brand_id, brand_name])

        print(index, "\t", brand_id)
        product_ids_by_brand.append(productID)

    if (currentPage * pageSize) < totalProducts:
        product_ids_by_brand.extend(get_productID_by_brand(writer, brand_id, currentPage + 1))

    return product_ids_by_brand

# crawler
# get the json data from sephora review API
# the api url is: https://api.bazaarvoice.com/data/reviews.json?Filter=ProductId%3A{P416375}&Sort=Helpfulness%3Adesc&Limit=100&Offset=0&Include=Products%2CComments&Stats=Reviews&passkey=rwbw526r2e7spptqd2qzbkp7&apiversion=5.4
# where the ID can be replaced and get the result as json
def get_reviews(productID, limit, offset, filedir):
    try:
        review_url = "https://api.bazaarvoice.com/data/reviews.json?Filter=ProductId%3A" + str(productID) + "&Sort=Helpfulness%3Adesc&Limit=" + str(limit) + "&Offset=" + str(offset) + "&Include=Products%2CComments&Stats=Reviews&passkey=rwbw526r2e7spptqd2qzbkp7&apiversion=5.4"  
        data=requests.get(review_url)
        json_data = json.loads(data.content)
        totalResults = json_data["TotalResults"]

        # decide if the totalResult doesn't have value
        if not totalResults is not None:
            return
        if totalResults == 0:
            return
        # print(totalResults)
        current_total = limit + offset
        threshold = 0
        if current_total > totalResults:
            threshold = totalResults - offset
        else:
            threshold = limit

        # for eliminate the ERROR: list index out of range 
        if threshold > len(json_data["Results"]):
            threshold = len(json_data["Results"])
        

        # wirte txt file to different directory, 30000 per directory
        global global_count
        global fileIndex
        # global csvfile
        # global writer
        filedir_fileIndex = os.path.join(filedir, str(fileIndex))
        mkdir(filedir_fileIndex)

        for index in range(threshold):
            userNickname = json_data["Results"][index]["UserNickname"]
            rating = json_data["Results"][index]["Rating"]
            review = json_data["Results"][index]["ReviewText"]
            if not userNickname is not None:
                userNickname = "NA"
            if not rating is not None:
                rating = "NA"
            if not review is not None:
                review = "NA"
            # print(offset + index,"\t",review)

            # "age", "eyeColor", "hairColor", "skinTone", "skinType"
            ContextDataValues = json_data["Results"][index]["ContextDataValues"]
            age, eyeColor, hairColor, skinTone, skinType = fetch_product_infos(ContextDataValues)
            
            if global_count % 30000 == 0 and not global_count == 0:
                fileIndex += 1
                # =================
                # csvfile.close()
                # csvfile = open(os.path.join(filedir, "Prducts_reviews_part" + str(fileIndex) + ".csv"), "a+")
                # writer = csv.writer(csvfile)
                # writer.writerow(["productID", "userNickname", "rating", "age", "eyeColor", "hairColor", "skinTone", "skinType", "review"])
                # =================
                filedir_fileIndex = os.path.join(filedir, str(fileIndex))
                mkdir(filedir_fileIndex)
            
            # write_review_to_csv(writer, productID, userNickname, rating, age, eyeColor, hairColor, skinTone, skinType, review)
            write_review_to_filedir(filedir_fileIndex, productID, userNickname, rating, age, eyeColor, hairColor, skinTone, skinType, review)
        if current_total >= totalResults:
            return
        else:
            get_reviews(productID, limit, offset + limit, filedir)
    except Exception:
        print("ERROR: ProductID: ", str(productID)," ---- Connection false!!!! ")
        

# "age", "eyeColor", "hairColor", "skinTone", "skinType"
def fetch_product_infos(ContextDataValues):
    if not ContextDataValues is not None:
        age = "NA"
        eyeColor = "NA"
        hairColor = "NA"
        skinTone = "NA"
        skinType = "NA"
    else:
        if "age" in ContextDataValues:
            age = ContextDataValues["age"]["ValueLabel"]
            if not age is not None:
                age = "NA"
        else:
            age = "NA"
        if "eyeColor" in ContextDataValues:
            eyeColor = ContextDataValues["eyeColor"]["ValueLabel"]
            if not eyeColor is not None:
                eyeColor = "NA"
        else:
            eyeColor = "NA"
        if "hairColor" in ContextDataValues:
            hairColor = ContextDataValues["hairColor"]["ValueLabel"]
            if not hairColor is not None:
                hairColor = "NA"
        else:
            hairColor = "NA"
        if "skinTone" in ContextDataValues:
            skinTone = ContextDataValues["skinTone"]["ValueLabel"]
            if not skinTone is not None:
                skinTone = "NA"
        else:
            skinTone = "NA"
        if "skinType" in ContextDataValues:
            skinType = ContextDataValues["skinType"]["ValueLabel"]
            if not skinType is not None:
                skinType = "NA"
        else:
            skinType = "NA"
    return age, eyeColor, hairColor, skinTone, skinType

# def write_review_to_filedir_with_index(filedir, index, data):
#     global global_count
#     write_file_name = str(global_count) + ".txt"

#     # for seeing the processing
#     print("global_count: ", global_count)
    
#     global_count += 1
#     write_file_path = os.path.join(filedir, write_file_name)
#     with open(write_file_path, "w") as f:
#         content = index + "\t" + data
#         f.write(content)

def write_review_to_filedir(filedir, productID, userNickname, rating, age, eyeColor, hairColor, skinTone, skinType, review):
    global global_count
    write_file_name = str(global_count) + ".txt"

    # for seeing the processing
    print("global_count: ", global_count, "======", "product_ID:", productID)

    global_count += 1
    write_file_path = os.path.join(filedir, write_file_name)
    with open(write_file_path, "w") as f:
        content = str(productID) + "\t" + userNickname + "\t" + str(rating) + "\t" + str(age) + "\t" + eyeColor + "\t" + hairColor + "\t" + skinTone + "\t" + skinType + "\t" + review
        f.write(content)

def write_review_to_csv(writer, productID, userNickname, rating, age, eyeColor, hairColor, skinTone, skinType, review):
    global global_count
    # for seeing the processing
    print("global_count: ", global_count, "======", "product_ID:", productID)
    writer.writerow([productID, userNickname, rating, age, eyeColor, hairColor, skinTone, skinType, review])
    global_count += 1
    
def mkdir(filedir):
    if not os.path.exists(filedir):
        os.makedirs(filedir)

# # Test 1
# url = "https://www.sephora.com/briogeo"
# brandID = get_brandID(url)
# print(brandID)
# exit()
# Test
# brand_ids = ['6208', '6245']
# brand_ids = ['3902', '6208']
# csvfile = open("Prducts_Info.csv", "a+")
# writer = csv.writer(csvfile)
# writer.writerow(["ProductID", "Brand_name", "Brand_id", "Brand_name"])
# productID_list = get_all_productID(brand_ids, writer)
# csvfile.close()
# print(productID_list, len(productID_list))
# exit()
# filedir = "Sephora_data"
# for productID in productID_list:
#     get_reviews(productID, LIMIT, 0, filedir)
# exit()
#

filedir = "Sephora/Sephora_data_csv"
mkdir(filedir)
brands_url = "https://www.sephora.com/brand/list.jsp"
brand_urls, brand_ids = get_brands_list(brands_url, filedir)
csvfile = open(os.path.join(filedir, "Prducts_Info.csv"), "a+")
writer = csv.writer(csvfile)
writer.writerow(["ProductID", "Product_name", "Brand_id", "Brand_name"])
productID_list = get_all_productID(brand_ids, writer)
csvfile.close()
print(productID_list, len(productID_list))
# ================== TEST ========================
# csvfile = open(os.path.join(filedir,"Prducts_reviews.csv"), "a+")
# writer = csv.writer(csvfile)
# writer.writerow(["productID", "userNickname", "rating", "age", "eyeColor", "hairColor", "skinTone", "skinType", "review"])
# get_reviews("P0249", LIMIT, 0, filedir)
# exit()
# csvfile.close()
# ================================================
# csvfile = open(os.path.join(filedir, "Prducts_reviews_part" + str(fileIndex) + ".csv"), "a+")
# writer = csv.writer(csvfile)
# writer.writerow(["productID", "userNickname", "rating", "age", "eyeColor", "hairColor", "skinTone", "skinType", "review"])
filedir = "Sephora/Sephora_data_reviews"
mkdir(filedir)
# get_reviews("P0249", LIMIT, 0, filedir)
# exit()
for productID in productID_list:
    try:
        get_reviews(productID, LIMIT, 0, filedir)
    except Exception:
        print("ERROR!!!!")
# csvfile.close()
