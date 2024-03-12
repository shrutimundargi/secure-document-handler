import os
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models.param import Param
from datetime import timedelta
import requests
from pypdf import PdfReader
from io import BytesIO
import re
import openai
import pandas as pd 
import os
import getpass
import json
from transformers import BertTokenizer, BertModel
import torch

user_input = {
    "datasource_url1": Param(default="https://www.sec.gov/files/form1.pdf", type='string', minLength=5, maxLength=255),
    "datasource_url2": Param(default="https://www.sec.gov/files/form1-a.pdf", type='string', minLength=5, maxLength=255),
    "datasource_url3": Param(default="https://www.sec.gov/files/form1-e.pdf", type='string', minLength=5, maxLength=255),
    "datasource_url4": Param(default="https://www.sec.gov/files/form1-k.pdf", type='string', minLength=5, maxLength=255),
    "datasource_url5": Param(default="https://www.sec.gov/files/form1-n.pdf", type='string', minLength=5, maxLength=255),
    "Processing_option": Param(default="Nougat or PyPDF", type='string', minLength=5, maxLength=255),
}


dag = DAG(
    dag_id="sandbox",
    schedule="0 0 * * *",   
    start_date=days_ago(0),
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
    tags=["labs", "damg7245"],
    params=user_input,
)



def dataExtraction(**kwargs):
    pdf_urls = [kwargs["params"]["datasource_url1"], kwargs["params"]["datasource_url2"], kwargs["params"]["datasource_url3"], kwargs["params"]["datasource_url4"], kwargs["params"]["datasource_url5"]]
    processing = kwargs["params"]["Processing_option"]
    truncated_pdf_urls = [kwargs["params"]["datasource_url1"], kwargs["params"]["datasource_url2"], kwargs["params"]["datasource_url3"], kwargs["params"]["datasource_url4"], kwargs["params"]["datasource_url5"]]

    if processing == 'PyPDF':
        def extract_pdf(link):
            response = requests.get(link)
            if response.status_code == 200:
                with BytesIO(response.content) as open_pdf_file:
                    reader = PdfReader(open_pdf_file)
                    text = ""
                    for page_num in range(len(reader.pages)):
                        text += reader.pages[page_num].extract_text()
            else:
                text = ""
            return text
        for i in range(0,len(pdf_urls)):
            if pdf_urls[i] != 'None':
                print(i)
                text = extract_pdf(pdf_urls[i])
                words = re.sub(r'[^a-zA-Z0-9 ]', ' ', text)
                cleaned_text = ' '.join(words.split())
                truncated_pdf_urls[i] = cleaned_text
            else:
                truncated_pdf_urls = ""
    # Set the maximum token limit
        def limit_string_length(text, max_tokens):
            tokens = text.split()
            truncated_tokens = tokens[:max_tokens]
            truncated_text = ' '.join(truncated_tokens)
            return truncated_text

        max_token_limit = 3000  # Adjust this to your desired limit

        truncated_pdf_urls = [limit_string_length(url, max_token_limit) for url in truncated_pdf_urls]
        openai.api_key = os.getenv('OPENAI_KEY')

        EMBEDDING_MODEL = "text-embedding-ada-002"  # OpenAI's best embeddings as of Apr 2023
        BATCH_SIZE = 1000  # you can submit up to 2048 embedding inputs per request

        embeddings = []
        for batch_start in range(0, len(truncated_pdf_urls), BATCH_SIZE):
            batch_end = batch_start + BATCH_SIZE
            batch = truncated_pdf_urls[batch_start:batch_end]
            print(f"Batch {batch_start} to {batch_end-1}")
            response = openai.Embedding.create(model=EMBEDDING_MODEL, input=batch)
            for i, be in enumerate(response["data"]):
                assert i == be["index"]  # double check embeddings are in same order as input
            batch_embeddings = [e["embedding"] for e in response["data"]]
            embeddings.extend(batch_embeddings)

        df1 = pd.DataFrame({"embedding": embeddings})
        metadata = []
        for i in range(0, len(truncated_pdf_urls)):
            metadata.append({
                "link": pdf_urls[i],
                "context": truncated_pdf_urls[i],
                "Processing_method": processing
            })

        # Save the metadata to a JSON file
        with open("/opt/airflow/dags/metadata.json", "w") as json_file:
            json.dump(metadata, json_file)

        df1.to_csv("/opt/airflow/dags/embedded_dataset.csv", index=False)
    
    return df1


with dag:
    hello_world = BashOperator(
        task_id="hello_world",
        bash_command='echo "Hello from airflow"'
    )

    dataExtraction = PythonOperator(
        task_id='dataExtraction',
        python_callable=dataExtraction,
        provide_context=True,
        dag=dag,
    )


    bye_world = BashOperator(
        task_id="bye_world",
        bash_command='echo "Bye from airflow"'
    )

    hello_world >> dataExtraction >>  bye_world
