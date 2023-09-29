
import os
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import requests
from selenium.webdriver.common.by import By
import pandas as pd
import boto3
from datetime import datetime
import time
from selenium.webdriver.common.action_chains import ActionChains
import logging

def setup_custom_logger():
    # Create a custom logger
    logger = logging.getLogger("loader") 
    # Create a formatter
    formatter = logging.Formatter(fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S') 
    # Create a file handler and set the formatter
    file_handler = logging.FileHandler("logs/")
    file_handler.setFormatter(formatter) 
    # Add the file handler to the logger
    logger.addHandler(file_handler) 
    # Set the logging level
    logger.setLevel(logging.INFO)
    return logger


logger = setup_custom_logger()


def extract(page_number):
    url = "https://tonaton.com/c_houses-apartments-for-rent?page=" + str(page_number)
    response = requests.get(url)
    pages_source = response.text;
    soup = BeautifulSoup(pages_source,"html.parser")
    logger.info("Page Entry Loaded")

    prices = soup.find_all(class_="product__title")
    for count,price in enumerate(prices,start=0):
       prices[count] = price.text
       logger.info("Prices Entry Extracted")
    
    locations = soup.find_all(class_="product__location")
    for count,location in enumerate(locations,start=0):
        locations[count] = location.text.replace('\n', '')
        logger.info("Location Entry Extracted")

    descriptions = soup.find_all(class_="product__description")
    for count,description in enumerate(descriptions,start=0):
        descriptions[count] = description.text.replace('\n', '')
        logger.info("Descriptions Entry Extracted")

    tags = soup.find_all(class_="product__tags")  
    for count,tag in enumerate(tags,start=0):
        tags[count] = tag.text.replace('\n', '')
        logger.info("Tags Entry Extracted")

    img_links = soup.find_all(class_="h-opacity-0_8")
    for count,img_link in enumerate(img_links,start=0):
       img_links[count] = img_link.get("src")
       logger.info("Images Entry Extracted")

    contact_divs = soup.find_all(class_="product__item flex")
    for count,contact_div in enumerate(contact_divs,start=0):
       contact_divs[count]="https://tonaton.com" + str(contact_div.get("href"))
    contacts=[]
    logger.info("Contacts Entry Extracted")

    landlords =  []
    for count,contact_div in enumerate(contact_divs,start=1):
        try:
            response = requests.get(contact_div)
            pages_source = response.text;
            soup = BeautifulSoup(pages_source,"html.parser")
            details_sides = soup.find('div',class_="details__side")
            details_sides_grids = details_sides.find_all('div')
            if len(details_sides_grids) >= 3:
                second_div = details_sides_grids[2]  # Index 1 corresponds to the second div
                landlord_div = second_div.find('div')
                landlord = landlord_div.find('p')
                landlords.append(landlord)
        except Exception as e:
            logger.info(f"Error: {e}")
            continue

    for count,landlord in enumerate(landlords,start=0):
        landlords[count] = landlord.text.replace('\n', '')
        logger.info("Landlord Entry extracted")
    

    result = []
    for i in range(len(prices)):
        data = {
            "price": prices[i],
            "location": locations[i],
            "description": descriptions[i],
            "tags":tags[i],
            "landlord":landlords[i],
            "img_links":img_links[i],
            "webpages": contact_divs[i]
            }
    logger.info("Data Row Entry Extracted")

    df = pd.DataFrame(result)

    file = "hata" + str(page_number) + ".csv"
    df.to_csv(file,index=False)

    # Get the current date and time
    current_datetime = datetime.now()
    formatted_date = current_datetime.strftime("%Y%m%d")
    key = 'data/tonaton/dataload='+str(formatted_date)+'/'+file
    logger.info("File Created")
    s3 = boto3.client('s3')
    s3.upload_file(file,'houserentalsdatalake',key)
    logger.info("File Uploaded")
    os.remove(file)
    logger.info("File Deleted")

try:
    for i in range(1,499):
        extract(i)
        logger.info(f"Iteration {str(i)} completed")
except Exception as e:
    logger.warning(e)