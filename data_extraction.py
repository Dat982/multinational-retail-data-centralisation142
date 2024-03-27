import pandas as pd
from database_utils import DatabaseConnector
import tabula
import boto3
import re
import requests


class DataExtractor:
	
	@staticmethod
	def read_rds_table(table_name, conn):
		df = pd.read_sql(table_name, con=conn)
		return df
	
	@staticmethod
	def retrieve_pdf_data(pdf_url):
		df_list = tabula.read_pdf(pdf_url, pages="all", multiple_tables=True)
		extracted_data = pd.concat(df_list, ignore_index=True)
		return extracted_data
	
	@staticmethod
	def extract_from_s3(path):
		s3_client = boto3.client('s3')
		path_split = re.split(r'/+', path)
		bucket = path_split[1]
		key = path_split[-1]
		response = s3_client.get_object(Bucket='data-handling-public', Key='products.csv')
		status   = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
		if status == 200:
			df = pd.read_csv(response.get("Body"))
			return df

	@staticmethod
	def extract_from_s3_by_link(url):
		response = requests.get(url) 
		dic	  = response.json()
		df	   = pd.DataFrame([])
		for column_name in dic.keys():
			value_list = []
			for _ in dic[column_name].keys():
				value_list.append(dic[column_name][_])
			df[column_name] = value_list
		return df
	
	@staticmethod
	def list_number_of_stores(url, headers=dict()):
		print('Getting number of stores from API: ' + url)
		r = DataExtractor.send_get_request(url, headers)
		num_stores = r.json()['number_stores']
		return num_stores

	@staticmethod
	def read_json(url, headers=dict()):
		print('Reading JSON data: ' + url)
		r = DataExtractor.send_get_request(url, headers)
		df = pd.DataFrame(r.json())
		return df


	@staticmethod
	def retrieve_stores_data(endpoint, headers, num_stores=1):
		print('Retrieving store details from API: ' + endpoint)
		store_data = []
		for store_no in range(num_stores):
			url = endpoint.format(store_no=store_no)
			r = DataExtractor.send_get_request(url, headers)
			store_data.append(r.json())
		df = pd.DataFrame(store_data)
		return df

	@staticmethod
	def send_get_request(url, headers=dict()):
		r = requests.get(url, headers=headers)
		assert r.status_code == 200, f'Request failed with status {r.status_code}'
		return r


if __name__ == '__main__':
	
	connector_instance = DatabaseConnector()
	cred = connector_instance.read_db_creds('db_creds.yaml')
	engine = connector_instance.init_db_engine(cred)
	with engine.connect() as conn:
		users = DataExtractor.read_rds_table('legacy_users', conn)
	
	print(users.head())
	
	
	url = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
	df = DataExtractor.retrieve_pdf_data(url)
	print(df.head())

	
	url = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
	headers = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
	num_stores = DataExtractor.list_number_of_stores(url, headers)

	
	url = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{num_stores}'
	df = DataExtractor.retrieve_stores_data(url, headers, num_stores)
	print(df.head())

	
	url = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{num_stores}'
	df = DataExtractor.retrieve_stores_data(url, headers, num_stores)
	print(df.head())


	s3_address = 's3://data-handling-public/products.csv'
	df = DataExtractor.extract_from_s3(s3_address)
	print(df.head())
	
	url = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'
	df = DataExtractor.read_json(url)
	print(df.head())
