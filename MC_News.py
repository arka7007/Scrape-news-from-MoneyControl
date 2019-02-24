from bs4 import BeautifulSoup
import requests, re
from collections import OrderedDict
from dateutil.parser import parse
import datetime
import logging
import time
import sys


####################################################################################################

"""
MoneyControl News scraper 
	
Description:
_____________

    * Functions that will be use to scrape Company specific news (Daily and Yearly) 
	* Find out the company alias from base url search
	* Find out news page
	* Scrape daily and yearly news
	
Input: 
____________

	Company tickers (ISIN, BSE, NSE), Company Name manual input
	
Output:
_____________

	News with Heading, Company name, Content, post date as json

Technical requirements: 
_______________	
	*language: python 
		*version: 3.5
	*database: MongoDB

***__auth__==ark@007
	"""


###################################### Library functions ######################################


"""
	Initializing console log
"""
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('MoneyCntrol News scrapping')


def money_control_base_url(search_key):
	"""
		Initializing base url of MoneyControl website

		Parameters
		_____________
			*param1: search_key
				*type: str

		Returns
		__________
			*return: url
				*type: str
	"""
	return "http://www.moneycontrol.com/stocks/cptmarket/compsearchnew.php?search_data=&cid=&mbsearch_str=&topsearch_type=1&search_str="+str(search_key)


def bulk_scrape_url(key, year):
	"""
		Initializing MoneyControl's url for bulk scraping

		Parameters
		____________
			*param1: key
				*type: str
			*param2: year
				*type: str

		Variables
		____________
			*var1: url
				*access: local
				*type: str
		
		Returns
		__________
			*return: url
				*type: str
	"""
	url = "https://www.moneycontrol.com/stocks/company_info/stock_news.php?sc_id="+key+"&durationType=Y&Year="+str(year)
	return url


def create_empty_list():
	"""
		Creating empty list

		Variables
		_____________
			*var1: new_list
				*access: local
				*type: list

		Returns
		__________
			*return: new_list
				*type: list
	"""
	new_list = []
	return new_list


def create_empty_dir():
	"""
		Creating empty dictionary

		Variables
		_____________
			*var1: new_dir
				*access: local
				*type: dict

		Returns
		__________
			*return: new_dir
				*type: dict
	"""
	new_dir = {}
	return new_dir


def get_response(url):
	"""
		Hit an url and get get response

		Request Method
		__________________
			* GET request
             
		External packages
		____________________
			*package1: requests
               
		Parameters
		_____________
			*param1: url
				*type: str

		Variables
		____________
			*var1: response
				*access: local
				*type: requests.models.Response
                
		Returns
		___________
			*return: response
				*type: requests.models.Response
	"""
	# s = requests.Session()
	# s.headers['User-Agent'] = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/34.0.1847.131 Safari/537.36'
	try:
		headers = {
	    'Accept-Encoding': 'gzip, deflate, sdch',
	    'Accept-Language': 'en-US,en;q=0.8',
	    'Upgrade-Insecure-Requests': '1',
	    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36',
	    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
	    'Cache-Control': 'max-age=0',
	    'Connection': 'keep-alive',
		}

		response = requests.get(url,headers=headers)
		time.sleep(10)
		return response
	except:
		return " "


def parse_response_to_text(response):
	"""
		Parse response to text

		External packages
		____________________
			*package: BeautifulSoup4

		Parameters
		_______________
			*param1: response
				*type: requests.models.Response

		Variables
		______________
			*var1: soup
				*access: local
				*type: str
		
		Returns
		____________
			*return: soup
				*type: str
	"""
	soup = BeautifulSoup(response.text, 'html.parser')
	return soup 


def get_all_link(soup):
	"""
		finding all links from text

		Parameters
		_____________
			*param1: soup
				*type: str

		Variables
		_____________
			*var1: link
				*access: local
				*type: list

		Returns
		_________
			*return: link
				*type: list 
	"""
	link = str(soup.find('link'))
	return link


def get_all_news_link(soup, all_links):
	"""
		Get all news links from text

		Parameters
		_____________
			*param1: soup
				*type: str
			*param2: all_links
				*type: list

		Returns
		__________
			*return: all_links
				*type: list
	"""
	for link in soup.findAll("a"):
		all_links.append(link.get("href"))
	return all_links


def get_bulk_news_link(link):
	"""
		Get links for bulk news scrapping

		Parameters
		_____________
			*param1: link
				*type: str

		Variables
		____________
			*var1: all_link
				*access: local
				*type: str
			*var2: news_link
				*access: local
				*type: str

		Returns
		__________
			*return: news_link
				*type: str 
	"""
	all_link = link.get("href")
	if "/news/" in all_link and '.html' in all_link and 'https' not in all_link and '#' not in all_link:
		news_link = "https://www.moneycontrol.com"+all_link
		return news_link


def pre_filter_news_link(all_news, all_links):
	"""
		Pre filtering news link, trimming out extra links (Advertisement etc)

		Parameters
		_____________
			*param1: all_news
				*type: list
			*param2: all_links
				*type: list

		Variables
		____________
			*var1: check_link
				*access: local
				*type: str
			*var2: all_news_link
				*access: local
				*type: list

		Returns
		_________
			*return: all_news_link
				*type: list
	"""
	for link in all_links:
		if link is not None and "/news/" in link and 'https' not in link:
			check_link = link.split('/')
			if check_link[1] =='news' and len(link)>60:
				all_news.append(link)
	all_news_link = list(OrderedDict.fromkeys(all_news))
	return all_news_link


def get_all_article_link(link, article):
	"""
		Creating article link

		Parameters
		_____________
			*param1: link
				*type: list
			*param2: article
				*type: list

		Variables
		_____________
			*var1: url
				*access: local
				*type: str

		Returns
		___________
			*return: article
				*type: list 
	"""
	for each in link:
		url = "https://www.moneycontrol.com"+str(each)
		article.append(url)
	return article
	

def get_search_string(link, bse, nse):
	"""
		Validate link for search company alias

		Parameters
		_____________
			*param1: link
				*type: str
			*param2: bse
				*type: str
			*param3: nse
				*type: str 

		Returns
		__________
			*return: link
				*type: str
	"""
	if 'stockpricequote' in link:
		return link
	else:
		link = get_all_link(bse)
		if 'stockpricequote' in link:
			return link
		else:
			link = get_all_link(nse)
			return link


def get_company_alias(search_string):
	"""
		Extract company alias from search string

		Parameters
		_____________
			*param1: search_string
				*type: str

		Variables
		____________
			*var1: sort
				*type: str
			*var2: key
				*type: str

		Returns
		__________
			*return: key
				*type: str
	"""
	sort = re.search('stockpricequote/(.+?)" rel', search_string).group(1)
	key = sort.split('/')[2]
	return key


def get_news_home_page_url(company_alias, bse):
	"""
		Initialize home page url for news

		Parameters
		_____________
			*param1: company_alias
				*type: str
			*param2: bse
				*type: str

		Variables
		____________
			*var1: url
				*access: local
				*type: str

		Returns
		____________
			*return: url
				*type: str

	"""
	url = "https://www.moneycontrol.com/company-article/"+str(bse)+"/news/"+str(company_alias)
	return url


def get_all_news_articles(company_alias, bse):
	"""
		Extract all news articles

		Parameters
		______________
			*param1: company_alias
				*type: str
			*param2: bse
				*type: str

		Variables
		____________
			*var1: all_links
				*access: local
				*type:list
			*var2: filtered_link
				*access: local
				*type: list
			*var3: article_link
				*access: local
				*type: list
			*var4: news_response
				*access: local
				*type: list
			*var5: url
				*access: local
				*type: str
			*var6: response
				*access: local
				*type: requests.models.Response
			*var7: soup
				*access: local
				*type: str
			*var8: all_links
				*access: local
				*type: list
			*var9: filtered_link
				*access: local
				*type: list
			*var10: atricle_link
				*access: local
				*type: list
		
		Returns
		______________
			*return: article_link
				*type: list

	"""
	all_links = create_empty_list()
	filtered_link = create_empty_list()
	article_link = create_empty_list()
	news_response = create_empty_list()
	url = get_news_home_page_url(company_alias, bse)
	response = get_response(url)
	soup = parse_response_to_text(response)
	all_links = get_all_news_link(soup, all_links)
	filtered_link = pre_filter_news_link(filtered_link, all_links)
	article_link = get_all_article_link(filtered_link, article_link)
	return article_link


def get_all_news_pages_response(article_link, news_response):
	"""
		Hit article news link and collect the responses

		Parameters
		_____________
			*param1: article_link
				*type: list
			*param2: news_response
				*type: list

		variables
		____________
			*var1: resp
				*access: local
				*type: requests.models.Response

		Returns
		___________
			*return: news_response
				*type: list
	"""
	for url in article_link:
		try:
			resp = get_response(url)
			time.sleep(10)
			news_response.append(resp)
		except:
			logger.error("=========== Unable to get response for News =========")
			sys.exit()
	return news_response


def get_header(text):
	"""
		Extract news Heading from text

		Parameters
		_____________
			*param1: text
				*type: str

		Variables
		____________
			*var1: header
				*access: local
				*type: str

		Returns
		____________
			*return: header
				*type: str
	"""
	header = re.search('"artTitle">(.*)</h1>', text).group(1)
	return header


def get_news_pub_date(text):
	"""
		Extract news published date from text

		Parameters
		_____________
			*param1: text
				*type: str

		Variables
		_____________
			*var1: pub_date
				*access: local
				*type: datetime

		Returns
		___________
			* return: pub_date
				*type: datetime
	"""
	try:

		pub_date =re.search('"arttidate ">Last Updated :(.*)IST', text).group(1)
		pub_date = parse(pub_date)
		print(" ----------------- Under try block -------------------")
		return pub_date
	except:
		pub_date =re.search('"arttidate">Last Updated :(.*)IST', text).group(1)
		pub_date = parse(pub_date)
		print(" ----------------- Under catch block -------------------")
		return pub_date


def get_news_content(text):
	"""
		Extract news content from text

		Parameters
		_____________
			*param1: text
				*type: str

		Variables
		____________
			*var1: content
				*access: local
				*type: str

		Returns
		___________
			*return: content
				*type: str
	"""
	try:
		text = re.search('"articleBody":(.*)"articleSection"', text)
		content = text.group(1).replace("\'", "").replace('"','')
	except:
		text = " "
	return content



def get_all_news_content(news_response):
	"""
		Collecting all news details

		Parameters
		_____________
			*param1: news_response
				*type: list

		Variables
		_____________
			*var1: news_list
				*access: local
				*type: list
			*var2: news_details
				*access: local
				*type: dict
			*var3: text
				*access: local
				*type: str

		Returns
		___________
			*return: news_list
				*type: list
	"""
	news_list = create_empty_list()
	for resp in news_response:
		news_details = create_empty_dir()
		text = parse_response_to_text(resp)
		text = str(text).replace('\n',' ').replace('\r', ' ')
		print("------------------------------")
		# print(text)
		news_details['post_date'] = get_news_pub_date(text)
		news_details['news_heading'] = get_header(text)
		news_details['news_content'] = get_news_content(text)
		news_list.append(news_details)

	return news_list
		

def daily_scrape(isin, bse, nse):
	"""
		Main function, will scrape daily news from MoneyControl based on Company Tickers

		Parameters
		____________
			*param1: isin
				*type: str
			*param2: bse
				*type: bse
			*param3: nse
				*type: str

		Returns
		___________
			*return: newses
				*type: list

	"""
	try:
		url = money_control_base_url(isin)
		logger.info("............ fetching base url ........")
	except:
		logger.error("......... Unable to fetching ..........")

	response = get_response(url)

	if response.status_code == 200:
		logger.info("......... succesfully got response ............")

		try:
			soup = parse_response_to_text(response)
			logger.info("........... parsing response succesfully ...............")
		except:
			logger.error("...........unable to parse response ...................")

		try:
			link = get_all_link(soup)
			logger.info("............ collected link succesfully ................")
		except:
			logger.error("........... unable to collect link ...........")

		try:
			search_string = get_search_string(link, bse, nse)
			logger.info("........... serach string got succesfully ...........")
		except:
			logger.error("........... unable to get search_string ...........")

		try:
			company_alias = get_company_alias(search_string)
			logger.info("........... got company alias succesfully ..............")
		except:
			logger.error("........... unable to get company alias ........... ")
		
		try:
			all_news_articles_link = get_all_news_articles(company_alias, isin)
			logger.info("...............got all news links page succesfully ..............")
		except:
			logger.error(".............. unable to fetching news links response page .................")

		try:
			all_news_response = create_empty_list()
			all_news_response = get_all_news_pages_response(all_news_articles_link, all_news_response)
			print(all_news_response)
			logger.info("........... All news response collected succesfully ....................... ")
		except:
			logger.error("...........Unable to collect news response ..........................")

		try:
			newses = get_all_news_content(all_news_response)
			logger.info("....................... News collected succesfully .........................")
			return newses
			
		except:
			logger.error("...................... Unable to collect news .............................")
			return ''

	else:
		logger.warning("........... MoneyCntrol Base Url is not working................")



def extract_bulk_news(link,company):
	"""
		Extarct bulk news from links

		Parameters
		_____________
			*param1: link
				*type: str
			*param2: company
				*type: str

		Variables
		___________
			*var1: response
				*access: local
				*type: requests.models.Response
			*var2: news
				*access: local
				*type: dict
			*var3: soup
				*access: local
				*type: str
			*var4: cleaned_text
				*access: local
				*type: str
			*var5: content
				*access: local
				*type: str

		Returns
		___________
			*return: news
				*type: dict
	"""
	print(link)
	response = get_response(link)
	news = {}
	if response.status_code == 200:
		soup = BeautifulSoup(response.text, 'html.parser')
		cleaned_text = str(soup).replace('\n',' ').replace('\r', ' ')
		content = re.search('"articleBody":(.*)"articleSection"', cleaned_text)
		news['company'] = company
		try:
			news['news_content'] = content.group(1).replace("\'", "").replace('"','')
		except:
			news['news_content'] = " "

		try:
			news['news_heading'] = re.search('"artTitle">(.*)</h1', cleaned_text).group(1)
		except:
			news['news_heading'] = " "

		try:
			news['source'] = 'MoneyCntrol'
		except:
			news['source'] = " "

		try:
			news['post_date'] = get_news_pub_date(cleaned_text)
		except:
			news['post_date'] = " "

		try: 
			news['scraped_date'] = datetime.datetime.now()
		except:
			news['scraped_date'] = " "

		return news
		

def bullk_scrape(isin, bse, nse, company, year):
	"""
		Bulk news scrapping from MoneyControl based on Company Tickers

		Parameters
		_____________
			*param1: isin
				*type: str
			*param2: bse
				*type: str
			*param3: nse
				*type: nse
			*param4: company
				*type: str
			*param5: year
				*type: str

		Returns
		___________
			*return: all_news
				*type: str
	"""
	url = money_control_base_url(isin)
	response = get_response(url)

	if response.status_code == 200:
		soup = parse_response_to_text(response)
		link = get_all_link(soup)
		search_string = get_search_string(link, bse, nse)
		com_alias = get_company_alias(search_string)
		bulk_url = bulk_scrape_url(com_alias, year)
		bulk_response = get_response(bulk_url)

		if bulk_response.status_code == 200:
			soup = BeautifulSoup(bulk_response.text, 'html.parser')
			s = soup.find_all("div", class_="FL rightCont")
			c = s[0].find_all('a', href=True)
			all_news_url = list(set(filter(None,[get_bulk_news_link(x) for x in c])))
			all_news = [extract_bulk_news(x,company) for x in all_news_url]
			return all_news			





# if __name__ == '__main__':
# 	isin = "502137"
# 	bse = ""
# 	nse = ""
# 	bullk_scrape(isin, bse, nse, 'Deccance Cement')






