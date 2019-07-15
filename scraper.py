import scraperwiki
import sqlite3
import os
from   bs4 import BeautifulSoup
import sys
import time
import re
from datetime import datetime, timedelta
from re import sub
from decimal import Decimal
from dateutil.parser import parse
import math
import requests
import urllib.parse as urlparse
import random

import setEnvs


def parseAskingPrice(aPrice):
	try:
		value = round(Decimal(sub(r'[^\d.]', '', aPrice)))
	except:
		value = 0
	return value
	
def saveToStore(data):
	scraperwiki.sqlite.execute("CREATE TABLE IF NOT EXISTS 'trvdata' ( 'propId' TEXT, link TEXT, title TEXT, address TEXT, price BIGINT, 'displayPrice' TEXT, image1 TEXT, 'pubDate' DATETIME, 'addedOrReduced' DATE, reduced BOOLEAN, location TEXT,hashTagLocation TEXT, postContent TEXT, CHECK (reduced IN (0, 1)), PRIMARY KEY('propId'))")
	scraperwiki.sqlite.execute("CREATE UNIQUE INDEX IF NOT EXISTS 'trvdata_propId_unique' ON 'trvdata' ('propId')")
	scraperwiki.sqlite.execute("INSERT OR IGNORE INTO 'trvdata' VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)", (data['propId'], data['link'], data['title'], data['address'], data['price'], data['displayPrice'], data['image1'], data['pubDate'], data['addedOrReduced'], data['reduced'], data['location'],data['hashTagLocation'],data['postContent']))

excludeAgents = []
if os.environ.get("MORPH_EXCLUDE_AGENTS") is not None:
	excludeAgentsString = os.environ["MORPH_EXCLUDE_AGENTS"]
	excludeAgents = excludeAgentsString.lower().split("^")

keywords = []
if os.environ.get("MORPH_KEYWORDS") is not None:
	keywordsString = os.environ["MORPH_KEYWORDS"]
	keywords = keywordsString.lower().split("^")

filtered_dict = {k:v for (k,v) in os.environ.items() if 'MORPH_URL' in k}
postTemplates = {k:v for (k,v) in os.environ.items() if 'ENTRYTEXT' in k}

sleepTime = 5
domain = ""

if os.environ.get("MORPH_DB_ADD_COL") is not None:
	if os.environ.get("MORPH_DB_ADD_COL") == '1':
		try:
			scraperwiki.sqlite.execute('ALTER TABLE trvdata ADD COLUMN hashTagLocation TEXT')
		except:
			print('col - hashTagLocation exists')
		try:
			scraperwiki.sqlite.execute('ALTER TABLE trvdata ADD COLUMN postContent TEXT')
		except:
			print('col - postContent exists')

if os.environ.get("MORPH_SLEEP") is not None:
	sleepTime = int(os.environ["MORPH_SLEEP"])

if os.environ.get("MORPH_DOMAIN") is not None:
	domain = os.environ["MORPH_DOMAIN"]
	
with requests.session() as s:
	s.headers['user-agent'] = 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.186 Safari/537.36'

	for k, v in filtered_dict.items(): 
		checkURL = v
		if os.environ.get('MORPH_DEBUG') == "1":
			print(checkURL)
			
		#if os.environ.get('MORPH_MAXDAYS') == "0":
		#	checkURL = checkURL.replace("added=24_hours&","")
			
		parsedURL = urlparse.urlparse(checkURL)
		params = urlparse.parse_qs(parsedURL.query)
		if 'page_size' in params:
			pageSize = int(params['page_size'][0])
		else:
			pageSize = 25
		
		r1 = s.get(checkURL)
		soup = BeautifulSoup(r1.content, 'html.parser')
		
		try:
			numOfResults = soup.find("span", {"data-test" : "results"}).text
			numOfResults = int(numOfResults)
			numOfPages = math.ceil(float(numOfResults)/pageSize)
		except:
			numOfPages = 0	
		page = 0
		while page < numOfPages:
			numResults=0
			numPreFeat=0
			numNormFeat=0
			numFeat=0
			
			if page > 0: #get next page
				newPageCheckURL = checkURL.split('page.', 1)[0]
				r1 = s.get(newPageCheckURL+"page."+str(page))
				soup = BeautifulSoup(r1.content, 'html.parser')
			
			searchResults = soup.find("ul", {"id" : "wrapper_listing"})
			matches = 0
			if searchResults is not None:		
				adverts = searchResults.findAll("li")
				numResults = len(adverts)
				
				for advert in adverts:
					reduced=False
					if advert.find("div", {"class" : "item uk js-item js-backToTrovit"}) is not None:
						advertMatch = {}

						advertDesc=advert.find("div", {"class" : "description"}).find("p").text

						if any(x in advertDesc.lower() for x in keywords): #check if Match

							postKey = random.choice(list(postTemplates))
							random.shuffle(list(postTemplates))
							agent = advert.find("small", {"class" : "source"}).find("span").text
							
							if any(x in agent.lower() for x in excludeAgents):
								continue;

							hashTagLocation = k.replace("MORPH_URL_","").replace("_"," ").title().replace(" ","")

							location = k.replace("MORPH_URL_","").replace("_"," ").title()

							propLink=advert.find("a", {"class" : "js-item-title"}).get('href')

							propId=advert.find("div", {"class" : "item uk js-item js-backToTrovit"}).get('data-id')

							title = advert.find("a", {"class" : "js-item-title"}).text
							
							address = advert.find("h5", {"itemprop" : "address"}).find("span").text

							if address is not None and "," in address and address.strip() != "":
								addressLastParts = address.split(',')[-2].strip().split(' ')
								addressLastPart = ' '.join(addressLastParts)

								location = addressLastPart.title()
								hashTagLocation = addressLastPart.replace("_"," ").title().replace(" ","")
							else:
								location = 'UK'
								hashTagLocation = 'UK'

							price = parseAskingPrice(advert.find("span", {"class" : "amount"}).text)
							displayPrice = advert.find("span", {"class" : "amount"}).text

							if advert.find("div", {"data-test" : "photos"}).find("img") is not None:
								image1 = advert.find("div", {"data-test" : "photos"}).find("img").get('src')
							else:
								image1 = ""

							addedOrReduced = datetime.now().date()
							advertMatch['propId'] = propId
							advertMatch['link'] = propLink
							advertMatch['title'] = title.replace('Just added','').strip()
							advertMatch['address'] = address
							advertMatch['price'] = price
							advertMatch['displayPrice'] = displayPrice.replace('Just added','').strip()
							advertMatch['image1'] = image1
							advertMatch['pubDate'] = datetime.now()
							advertMatch['addedOrReduced'] = addedOrReduced
							advertMatch['reduced'] = reduced
							advertMatch['location'] = location
							advertMatch['hashTagLocation'] = hashTagLocation
							advertMatch['postContent'] = postTemplates[postKey].format(title, hashTagLocation, displayPrice)

							saveToStore(advertMatch)
							
							matches += 1
				print("Found "+str(matches)+" Matches from "+str(numResults)+" Items of which "+str(numFeat)+" are Featured")
				if matches == 0:
					break		
			else:
				print('No Search Results\n')
			page +=1 
		time.sleep(sleepTime)
sys.exit(0)
