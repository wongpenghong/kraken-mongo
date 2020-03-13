import pymongo
import ndjson
import json
import pandas as pd
from datetime import datetime
import dateutil 
import re
import os
from os import sys
from google.cloud import bigquery
from google.cloud import storage

# get credentials
with open("config_mongodb.json", "r") as read_file:
	CONFIG = json.load(read_file)['config_mongodb']

# set up service account	
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CONFIG['service_account']


class mongo_crawling:
	
	def __init__(self):
		self.connection = CONFIG['connection']
		self.database = CONFIG['database']
		self.collection = 'leadsTimeInteraction'
		self.date = sys.argv[1]
		self.client = bigquery.Client(project=CONFIG['project'])
		self.date_nodash = sys.argv[2]
		self.path_new = '{your_path}'
		self.path_exist = '{your_path}')
  
	def load_data_to_bq(self,table,path,date_nodash,condition): 
			"""
			loads or initalizes the job in BQ
			"""
			# for newest data
			if condition == 1:
				bigquery_client = bigquery.Client(CONFIG['project'])
				# print(bigquery_client)
				destination_dataset_ref = bigquery_client.dataset(CONFIG['dataset'])
				destination_table_ref = destination_dataset_ref.table(table + '$' + date_nodash)
				job_config = bigquery.LoadJobConfig()
				job_config.create_disposition = bigquery.CreateDisposition.CREATE_IF_NEEDED
				job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
				job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
				#using partition by ingestion Time
				job_config.time_partitioning = bigquery.TimePartitioning(type_=bigquery.TimePartitioningType.DAY)
				
				with open(path, 'rb') as f:
					job = bigquery_client.load_table_from_file(f, destination_table_ref, job_config=job_config)
					job.result()
					print('----->>>> '+path+' has success to load!')
					
			# for existing data
			elif condition == 2:
				bigquery_client = bigquery.Client(CONFIG['project'])
				# print(bigquery_client)
				destination_dataset_ref = bigquery_client.dataset(CONFIG['dataset']+'_temp')
				destination_table_ref = destination_dataset_ref.table(table + '$' + date_nodash)
				job_config = bigquery.LoadJobConfig()
				job_config.create_disposition = bigquery.CreateDisposition.CREATE_IF_NEEDED
				job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
				job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
				#using partition by ingestion Time
				job_config.time_partitioning = bigquery.TimePartitioning(type_=bigquery.TimePartitioningType.DAY)
				
				with open(path, 'rb') as f:
					job = bigquery_client.load_table_from_file(f, destination_table_ref, job_config=job_config)
					job.result()
					print('----->>>> '+path+' has success to load!')
	 
	def execute_merge_query(self,query, project_id=CONFIG['project']): # get data from bigquery
	
		client = bigquery.Client(project=project_id)
		df = client.query(query)
		return df
			
	def upload_blob(self,bucket_name, source_file_name, destination_blob_name):
		"""Uploads a file to the bucket."""
		
		storage_client = storage.Client()
		bucket = storage_client.get_bucket(bucket_name)
		blob = bucket.blob(destination_blob_name)
		print('success')
		blob.upload_from_filename(source_file_name)

		print('File {} uploaded to {}.'.format(
			source_file_name,
			destination_blob_name))
		os.remove(source_file_name)
		
		return 'success'
	
	def get_bq(self,data,project,dataset,table):
		query = (
			"""
			SELECT
				user_id
			FROM
				`{project}.{dataset}.{table}`
			WHERE
				user_id IN {data}
			""".format(data=data,project=project,dataset=dataset,table=table)
		)
		df = self.client.query(query).to_dataframe()
		
		return df
			
	
	 # @staticmethod
	def _build_connection(self):
		"""
		setting up the MongoDB connection
		:return: database type object
		"""
		client = pymongo.MongoClient(self.connection)
		return client
	
	def get_collection(self):
		"""
		get collection MongoDB Database
		:return: collection data
		"""
		client = self._build_connection()
		db = client[self.database]
		collection = db['leadsTimeInteraction']
		return collection
	
	def transform_date(self,df):
		#--------------------------------------------------------------------------------
		# transform format datetime
		if 'startTime' in df:
			df['startTime'] = pd.to_datetime(df['startTime'], format='%Y-%m-%d %H:%M:%S')
			df['startTime'] = df['startTime'].dt.strftime('%Y-%m-%d %H:%M:%S')
		
		if 'endTime' in df:
			df['endTime'] = pd.to_datetime(df['endTime'], format='%Y-%m-%d %H:%M:%S')
			df['endTime'] = df['endTime'].dt.strftime('%Y-%m-%d %H:%M:%S')
		
		if 'startSecondLoanTime' in df:
			df['startSecondLoanTime'] = pd.to_datetime(df['startSecondLoanTime'], format='%Y-%m-%d %H:%M:%S')
			df['startSecondLoanTime'] = df['startSecondLoanTime'].dt.strftime('%Y-%m-%d %H:%M:%S')
		
		if 'endSecondLoanTime' in df:
			df['endSecondLoanTime'] = pd.to_datetime(df['endSecondLoanTime'], format='%Y-%m-%d %H:%M:%S')
			df['endSecondLoanTime'] = df['endSecondLoanTime'].dt.strftime('%Y-%m-%d %H:%M:%S')
		
		if 'updatedAt' in df:
			df['updatedAt'] = pd.to_datetime(df['updatedAt'], format='%Y-%m-%d %H:%M:%S')
			df['updatedAt'] = df['updatedAt'].dt.strftime('%Y-%m-%d %H:%M:%S')  
		
		# transform dataframe to JSON
		datas = df.to_json(orient='records')
		# transform NaT to None type object
		transform = re.sub('"NaT"', 'null',datas)
		json_data = json.loads(transform)
		
		return json_data
	
	def get_arr(self,cursor):
		arr = []
		for i in cursor:
			if('user_id' in i):
				i['user_id'] = str(i['user_id'])
				arr.append(i)
		return arr
		
	def get_data(self,query,limit=0,verbose=1,condition=0):
		"""
		"""
		collection = self.get_collection()
		print(collection)
		
		# crate datetime for condition query in mongo
		startDate = dateutil.parser.parse("{start_date}T00:00:00.11Z".format(start_date = self.date))
		endDate = dateutil.parser.parse("{end_date}T23:59:59.11Z".format(end_date=self.date))
		
		# extract data and transform data
		if condition == 0:
			if(limit == 0):
				cursor = collection.find({"updatedAt":{'$gte':startDate,'$lte':endDate}},query)
				arr = self.get_arr(cursor)
				if arr == []:
					cursor = collection.find({"startTime":{'$gte':startDate,'$lte':endDate}},query)
					arr = self.get_arr(cursor)
			elif(limit<1):
				raise ValueError("Limit is not valid")
			else:
				cursor = collection.find({"updatedAt":{'$gte':startDate,'$lte':endDate}},query).limit(limit)
				arr = self.get_arr(cursor)
				
		elif condition == 1:
			if(limit == 0):
				cursor = collection.find({},query)
				arr = self.get_arr(cursor)
			elif(limit<1):
				raise ValueError("Limit is not valid")
			else:
				cursor = collection.find({},query).limit(limit)
				arr = self.get_arr(cursor)

		# transform data to dataframe
		df = pd.DataFrame(arr)
		df = df.drop_duplicates(subset=['user_id'])

		data = df.to_json(orient='records')
		json_data = json.loads(data)
		
		return json_data
	
	def transform_dict_to_df_dataLake(self):
		query = {'_id':0}
		data = self.get_data(query)
		arr_data = []
		
		for datas in data:
			# format createdAt to datetime
			tempDictData = {
				'user_id': datas['user_id'],
				'updatedAt': None,
				'startTime':None,
				'endTime': None,
				'startSecondLoanTime': None,
				'endSecondLoanTime': None,
			}
					
			# transform createdAt and updatedAt to dict of array
				
			if('updatedAt' in datas and datas['updatedAt'] != None):
				updatedAtFormat = pd.to_datetime(datas['updatedAt'],unit='ms')
				tempDictData['updatedAt'] = updatedAtFormat
			elif('updatedAt' not in datas):
				tempDictData['updatedAt'] = '{date} 00:00:00'.format(date=self.date)
			
			if('startTime' in datas and datas['startTime'] != None):
				startTimeFormat = pd.to_datetime(datas['startTime'],unit='ms')
				tempDictData['startTime'] = startTimeFormat
			elif('startTime' not in datas):
				tempDictData['startTime'] = '{date} 00:00:00'.format(date=self.date)
			
			if('endTime' in datas and datas['endTime'] != None):
				endTimeFormat = pd.to_datetime(datas['endTime'],unit='ms')
				tempDictData['endTime'] = endTimeFormat
			
			if('startSecondLoanTime' in datas and datas['startSecondLoanTime'] != None):
				startSecondLoanTimeFormat = pd.to_datetime(datas['startSecondLoanTime'],unit='ms')
				tempDictData['startSecondLoanTime'] = startSecondLoanTimeFormat
			
			if('endSecondLoanTime' in datas and datas['endSecondLoanTime'] != None):
				endSecondLoanTimeFormat = pd.to_datetime(datas['endSecondLoanTime'],unit='ms')
				tempDictData['endSecondLoanTime'] = endSecondLoanTimeFormat
	
			arr_data.append(tempDictData)
			
		# transform dict of array to dataframe    
		df = pd.DataFrame(arr_data)
		df = df.astype(str)

		return df

	def dump_json_file(self,data,path):
		# dump DF to NDJSON
		with open(path, 'w') as f:
			ndjson.dump(data, f)
	
		return True
	
	def leads_data_mart(self,verbose=1):
		if verbose:
			print('executing Process....')
			start = datetime.now()
		
		# get DataLake
		df = self.transform_dict_to_df_dataLake()
		user_id_mongoDB = df.user_id
		# Get user_id in BigQuery
		list_user_id = tuple(x for x in user_id_mongoDB.values)
		data_df_BQ = self.get_bq(list_user_id,project,dataset,table)
		user_id_BQ = data_df_BQ.user_id
		
		if(df.empty == False):
			# create DataMart
			leads_time_interaction = { 'user_id': df.user_id, 'startTime': df.startTime, 'updatedAt': df.updatedAt, 'endTime': df.endTime, 'startSecondLoanTime': df.startSecondLoanTime, 'endSecondLoanTime': df.endSecondLoanTime } 
			df_leads_time_interaction = pd.DataFrame(leads_time_interaction) 
			
			#Split DF
			exist_df_time_interaction = df_leads_time_interaction[df_leads_time_interaction.user_id.isin(user_id_BQ)]
			new_df_time_interaction = df_leads_time_interaction[~df_leads_time_interaction.user_id.isin(user_id_BQ)]
			print('-------->>>> success split DF')
			
			# transform dataframe to JSON
			json_data_new_time_interaction = self.transform_date(new_df_time_interaction) 
			json_data_exist_time_interaction = self.transform_date(exist_df_time_interaction) 		
   
			# dump to NDJSON File
			self.dump_json_file(json_data_new_time_interaction,self.path_new)
			self.dump_json_file(json_data_exist_time_interaction,self.path_exist)
			print('----------->>>>>> success Dump File Data')
			
			# Load NDJSON to BQ  
			self.load_data_to_bq('{your_table}',self.path_new,self.date_nodash,1)
			self.load_data_to_bq('{your_table}',self.path_exist,self.date_nodash,2)
			print('----------->>>>>> success Load Data')
   
			# merge data BigQuery
			query_time_interaction = """
			MERGE `{dataset}.{table}` T
			USING `{leads}.{table}` S
			
			ON T.user_id = S.user_id
			WHEN MATCHED THEN
			UPDATE SET
				{your_column}
			"""
			self.execute_merge_query(query_time_interaction)
			print('------------>>>> success update data')
   
			# Delete Time Interaction Temp Data
			query = """
			DELETE FROM
				`{dataset}.{table}`
			WHERE DATE(_PARTITIONTIME) = {date}
			""".format(date=self.date)
			self.execute_merge_query(query)
			
			print('--------->>>> Delete Time Interaction Temp Data')

			# upload to GCS
			bucket_name = '{bucket}'
			destination_blob_name_new = '{path}'
			destination_blob_name_exist = '{path}'
			
			#Load JSON to GCS
			self.upload_blob(bucket_name,self.path_new,destination_blob_name_new)
			self.upload_blob(bucket_name,self.path_exist,destination_blob_name_exist)
			print('------------>>>> success upload to gcs')

		if verbose:
			print('Process executed in ' + str(datetime.now() - start))