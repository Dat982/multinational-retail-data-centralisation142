import yaml
import psycopg2
import pandas as pd
from sqlalchemy import create_engine, inspect

class DatabaseConnector:

	@staticmethod
	def read_db_creds(yaml_file):
		with open(yaml_file, 'r') as file:
			cred = yaml.safe_load(file)
		return cred
	
	@staticmethod
	def init_db_engine(cred):
		connection_string = f"{'postgresql'}+{'psycopg2'}://{cred['RDS_USER']}:{cred['RDS_PASSWORD']}@{cred['RDS_HOST']}:{cred['RDS_PORT']}/{cred['RDS_DATABASE']}"
		engine = create_engine(connection_string)
		return engine

	@staticmethod
	def list_db_tables(engine):
		inspector = inspect(engine)
		return inspector.get_table_names()
	
	@staticmethod    
	def upload_to_db(df, table_name, engine):
		df.to_sql(table_name, engine, if_exists='replace')

if __name__ == '__main__':
	connector_instance = DatabaseConnector()
	cred = connector_instance.read_db_creds('db_creds.yaml')
	engine = connector_instance.init_db_engine(cred)
	with engine.connect() as conn:
		db_tables = connector_instance.list_db_tables(engine)
		user_table = pd.read_sql(db_tables[1], con = conn)
	print(db_tables)
	print(user_table)
	