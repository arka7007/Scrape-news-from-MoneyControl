import MC_News as mcn 
import datetime
import database_connect as db_con
import pymongo 




####################################################################################################

"""
MoneyControl News scraper 
	
Description:
_____________

    * Script will scrape MoneyControl daily and yearly news by Company tickers
	* Find out the company alias from base url search
	* Find out news page
	* Scrape daily and yearly news
	* If not exist then push scraped news to MongoDb
	
Input: 
____________

	Company tickers (ISIN, BSE, NSE), Company Name, Year manual input
	
Output:
_____________

	News with Heading, Company name, Content, post date as MongoCollection

Technical requirements: 
_______________	
	*language: python 
		*version: 3.5
	*database: MongoDB

***__author__==ark@007
	"""


###################################### Script ########################################

"""
	Variables
	______________
		*var1: isin
			*access: global
			*type: str
		*var2: bse
			*access: global
			*type: str
		*var4: nse
			*access: global
			*type: str
		*var5: year
			*access: global
			*type: str
		*var6: company
			*access: global
			*type: str
		*var7: con
			*access: global
			*type: connection_string
		*var8: check
			*access: global
			*type: mongo_cursor
		*var9: news_details
			*access: global
			*type: list
		*var10: t
			*access: global
			*type: mongo_cursor
"""


isin = "502137"
bse = ""
nse = ""
year = ["2018", "2017", "2016", "2015", "2014"]
company = "Deccance Cement"

con = db_con.get_con()

check = con.news.find_one({"company": company})

if check:
	print("   Mission Accomplished   ")
	news_details = mcn.daily_scrape(isin, bse, nse)
	for all_news in news_details:
		all_news['company'] = company
		all_news['scraped_date'] = datetime.datetime.now()
		all_news['source'] = "Money Control"

		t = con.news.find_one({"company": company,"source":"Money Control", "news_heading": all_news['news_heading'], "post_date":all_news['post_date']})

		if t:
			print("===== Already exist in DB =====")
		else:
			con.news.insert_one(all_news)
			print("=============== Inserting into database ================")
else:
	for each in year:
		news_details = mcn.bullk_scrape(isin, bse, nse, company,each)
		print("   bulk inserting   ")
		for all_news in news_details:
			con.news.insert_one(all_news)

print("******************** News Collected and Inserted into Data Base *************************")
