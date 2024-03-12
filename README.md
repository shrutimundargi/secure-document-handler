# secure-document-handler
Text retrieval from documents and storing them using vector database

Secure Document Handler is an integrated solution designed to automate the process of document embedding creation and facilitate secure, efficient searches through large-scale document databases using advanced vector search techniques. This project leverages the power of Airflow for data pipelining, Pinecone for vector database services, and integrates user authentication with JWT for secure access.

## Features

Data Acquisition and Embedding Generation: Automated pipelines to process documents and generate embeddings using either Nougat or PyPdf processing options. 

Vector Database Operations: Parameterized insertion of records into Pinecone's vector database, with capabilities to create, update, and delete indexes. Secure User Authentication: Streamlined user registration and login process safeguarded by JWT, ensuring that only authenticated users can access protected endpoints. Client-Facing Application: A Streamlit-based interface that offers users the ability to register, login, and query information from preprocessed forms.


## Architecture

<img width="601" alt="Screenshot 2024-03-11 at 8 46 33 PM" src="https://github.com/shrutimundargi/secure-document-handler/assets/48567754/7079b478-b1e1-487c-abce-0a841a8880cc">

This project is a seamless blend of automation and AI, designed to streamline the ingestion and retrieval of information from PDF documents with a robust, cloud-hosted pipeline. It merges the power of Airflow with Pinecone's vector search, wrapped in a secure, user-friendly interface through FastAPI and Streamlit. 

Our goal: to revolutionize how users store, search, and interact with data, making the process as intuitive as asking a question.
