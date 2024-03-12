from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models.param import Param
from airflow.models import Variable
import pandas as pd
import pinecone
import os
import json
import time


pinecone_params = {
        'source_path': '/opt/airflow/dags/embedded_dataset.csv',
        'pinecone_api_key': Param(os.getenv('API_KEY'), type='string', description='Your Pinecone API key'),
        'pinecone_environment': Param('', type='string', description='Pinecone environment'),
        'pinecone_index_name': Param('', type='string', description='Your Pinecone index name'),
        'vector_field': Param('form_embeddings', type='string', description='The vector field in the CSV'),
        }


# Function to load CSV data into Pinecone
def load_csv_into_pinecone(**kwargs):
    params = kwargs['params']
    source_path = params['source_path']
    api_key = params['pinecone_api_key']
    environment = params['pinecone_environment']
    index_name = params['pinecone_index_name']
    vector_field = params['vector_field']
    metadata_fields = []
    # Initialize Pinecone
    pinecone.init(api_key=api_key, environment=environment)

   # Delete the index, if an index of the same name already exists
    if index_name in pinecone.list_indexes():
      pinecone.delete_index(index_name)
   
    dimensions = 768
    pinecone.create_index(name=index_name, dimension=dimensions, metric="euclidean")

    # wait for index to be ready before connecting
    while not pinecone.describe_index(index_name).status['ready']:
      time.sleep(1)
 
    index = pinecone.Index(index_name=index_name)
    
    df = pd.DataFrame()

    df['embeddings_1'] = []
 
    
 
    id = ['A','B','C','D','E']

    index.upsert(vectors=zip(id,df.embeddings_1,metadata_fields))
 


    # Function to transform a dataframe row to a Pinecone upsert request
    '''def row_to_upsert_request(row):
        # Fetch the metadata for the row using the column names
        metadata = {field: row[field] for field in metadata_fields}
        vector = row[vector_field].tolist() if isinstance(row[vector_field], (list, pd.Series)) else [row[vector_field]]
        return (str(row.name), vector, metadata)
    
    # Apply the function across the dataframe
    upsert_requests = df.apply(row_to_upsert_request, axis=1).tolist()
    
    # Perform the upserts
    for request in upsert_requests:
        id, vector, metadata = request
        index.upsert(vectors=zip(id, vector, metadata))'''
    
    print(f"Loaded CSV data from {source_path} into Pinecone index '{index_name}'.")


# Function to create/update Pinecone index
def manage_pinecone_index(**kwargs):
    params = kwargs['params']
    api_key = params['pinecone_api_key']
    environment = params['pinecone_environment']
    index_name = params['pinecone_index_name']
    
    # Initialize Pinecone
    pinecone.init(api_key=api_key, environment=environment)
    
    # Create or update the Pinecone index
    if index_name not in pinecone.list_indexes():
        pinecone.create_index(name=index_name)
        print(f"Created Pinecone index '{index_name}'.")
    else:
        # Here you would put any logic required to update the index, if necessary
        print(f"Pinecone index '{index_name}' already exists.")

# Function to delete Pinecone index
'''def delete_pinecone_index(**kwargs):
    params = kwargs['params']
    api_key = params['pinecone_api_key']
    environment = params['pinecone_environment']
    index_name = params['pinecone_index_name']
    
    # Initialize Pinecone
    pinecone.init(api_key=api_key, environment=environment)

    # Delete the Pinecone index
    pinecone.delete_index(index_name)
    print(f"Deleted Pinecone index '{index_name}'.")

'''
# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 11, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Define the DAG
dag = DAG(
    'pinecone_data_pipeline_with_params',
    default_args=default_args,
    description='A DAG to load data into Pinecone with parameterized CSV path',
    schedule_interval=None,  # Run manually
    catchup=False,
    tags=['pinecone', 'data loading'],
    params=pinecone_params, 

)

# Use the Params feature to parameterize the CSV source path
with dag:
    
    load_task = PythonOperator(
        task_id='load_csv_into_pinecone',
        python_callable=load_csv_into_pinecone,
        op_kwargs={'source_path': Variable.get('source_csv_path', default_var='/opt/airflow/dags/embedded_dataset.csv')},
    )

    create_update_task = PythonOperator(
        task_id='manage_pinecone_index',
        python_callable=manage_pinecone_index,
        op_kwargs=pinecone_params,
    )

    """delete_task = PythonOperator(
        task_id='delete_pinecone_index',
        python_callable=delete_pinecone_index,
        op_kwargs=pinecone_params,
    )"""

    # Define the sequence in which the tasks should be executed
    load_task >> create_update_task 

# The DAG object we'll be using to access the parameters
dag = dag
