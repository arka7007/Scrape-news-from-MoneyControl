import pymongo
from configparser import SafeConfigParser



"""
MoneyControl News scraper 
	
Description:
_____________

    * Funcions will connect mongo db
	
Input: 
____________

	config.ini file consisting momgo url, port, username, password, database name
	
Output:
_____________

	MongoDb connextion
	
Technical requirements: 
_______________	
	*language: python 
		*version: 3.5
	*database: MongoDB
***__author__==ark@007
	"""


###################################### Functions ########################################

def get_url():
	"""
		Reading config.ini and initialize MongoDb url

		Variables
		___________
			*var1: config
				*access: local
				*type: configparser.SafeConfigParser
			*var2: url
				*access: local
				*type: str

		Returns
		__________
			*return: url
				*type: str 
	"""

	config = SafeConfigParser()
	config.read('config.ini')
	url = config.get('Mongo_Config', 'url')
	return url


def get_port():
	"""
		Reading config.ini and initialize MongoDb port

		Variables
		___________
			*var1: config
				*access: local
				*type: configparser.SafeConfigParser
			*var2: port
				*access: local
				*type: int

		Returns
		__________
			*return: port
				*type: int 
	"""
	config = SafeConfigParser()
	config.read('config.ini')
	port = config.get('Mongo_Config', 'port')
	return int(port)


def get_db_name():
	"""
		Reading config.ini and initialize MongoDb database

		Variables
		___________
			*var1: config
				*access: local
				*type: configparser.SafeConfigParser
			*var2: db
				*access: local
				*type: str

		Returns
		__________
			*return: url
				*type: str 
	"""
	config = SafeConfigParser()
	config.read('config.ini')
	db = config.get('Mongo_Config', 'database')
	return db


def connect_mongo_db(url, port):
	"""
		Connecting MongoDb server

		Parameters
		____________
			*param1: url
				*type: str
			*param2: port
				*type: int
		Return
		__________
			*return: mongo connection
				*type: ConnectionString
	"""
	return pymongo.MongoClient(url, port)


def connect_database(db_name, db_connect):
	"""
		Connecting MongoDb database

		Parameters
		____________
			*param1: db_name
				*type: str
			*param2: db_connect
				*type: ConnectionString

		Variables
		___________
			*var1: database
				access: local
				type: DataBaseConnectionString

		Return
		__________
			*return: database
				*type: DataBaseConnectionString
	"""
	database = db_connect[db_name]
	return database


def get_con():
	"""
		Main functio to connect MongoDb database

		Variables
		___________
			*var1: url
				*access: local
				*type: str
			*var2: port
				*access: local
				*type: int
			*var3: db_name
				*access: local
				*type: str
			*var4: conn
				*access: local
				*type: ConnectionString
			*var5: db_conn
				*access: local
				*type: DataBaseConnectionString

		Returns
		__________
			*return: db_conn
				*type: DataBaseConnectionString 
	"""
	url = get_url()
	port = get_port()
	db_name = get_db_name()
	conn = connect_mongo_db(url, port)
	db_con = connect_database(db_name, conn)
	return db_con




