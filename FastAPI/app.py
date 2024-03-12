import streamlit as st
import mysql.connector
import requests
import os
import pinecone
import json
from requests.exceptions import RequestException



st.title("Login Page")

# Create input widgets for username and password
username = st.text_input("Username")
password = st.text_input("Password", type="password")

# Create a login button
login_button = st.button("Login")
signup_button = st.button("Sign up")

if not 'is_logged_in' in st.session_state:
    st.session_state.is_logged_in = False

 
# Check if the user is logged in
if login_button:
    data = {'username': username, 'password': password}
    response = requests.post('http://0.0.0.0:8000/token', data=data, headers=None)
    token_response = response.json()
    access_token = token_response['access_token']
    if response.status_code == 200:
        st.session_state.is_logged_in = True

if signup_button:
    data = {'username': username, 'hashed_password': password}
    response = requests.post('http://0.0.0.0:8000/users/register', json=data)
    
    if response.status_code == 200:
        token_response = response.json()
        user_token = token_response['username']
        st.success(f"User {user_token} created successfully.")
    else:
        # If there's an error, show it in the Streamlit app
        try:
            response_data = response.json()  # Attempt to get JSON error message
            error_message = response_data.get('detail', 'An error occurred.')
        except ValueError:
            # If response is not in JSON format, fall back to text
            error_message = response.text or 'An error occurred.'
        st.error(f"Failed to create user: {error_message}")

 
if st.session_state.is_logged_in:
        st.image('DocuAI.png')
        selected_tab = st.sidebar.selectbox("Select a tab:", ["Try", "Docs"])
 
        pinecone.init(api_key='',environment='')
        keys = ['A', 'B', 'C', 'D', 'E']
        values = ['form1', 'form1-a', 'form1-e', 'form1-k', 'form1-n']
        index_name = "hello"
        index = pinecone.Index(index_name=index_name)
        my_dict = dict(zip(values, keys))
 
        # Create the dictionary
        if selected_tab == "Try":
            try:
                st.title('Similarity Search using Pinecone')
                #auth jwt
                #index = pinecone.Index('hello')
                selected_value = st.selectbox("Select a value:", list(my_dict.keys()))
                # Display the selected value
                if selected_value:
                    selected_form = my_dict[selected_value]
                    st.write(f"You selected: {selected_value}")
                search_query = st.text_input('Enter your link:')
                a = index.query(
                    id = f"{selected_form}",
                    filter={
                            "link": {"$eq":f"{search_query}"},
                            "Processing_method": {"$eq": "PyPDF"}
                        },
                        top_k=1,
                        include_metadata=True
                )
                st.write(a['matches'][0]['metadata']['context'])
                        
        
            except Exception as e:
                # Handle the exception
                st.write("Enter URL Above")
        
        elif selected_tab == "Docs":
            st.title("DOCUMENTATION FOR NOUGHAT AND PYPDF")
            st.subheader("NOUGHAT PROS AND CONS:")
            st.write("""
                    **PROS:**
                    - Better handling of images and embedded objects within the PDF
                    - Provides more contextual summaries for longer documents
                    """)
            st.write("""
                    **CONS:**
                    - Slower processing time as compared to pypdf
                    - Some compatibility issues with heavily formatted documents
                    """)
            st.subheader("PYPDF PROS AND CONS:")
            st.write("""
                    **PROS:**
                    - Faster processing speeds for text-heavy documents
                    - Simple to use and has a straightforward integration with Streamlit
                    """)
            st.write("""
                    **CONS:**
                    - Cannot effectively handle images or embedded objects
                    - Summaries can occasionally miss crucial information for very lengthy documents
                    """)
            st.subheader("ARCHITECTURE DIAGRAM")
            st.image('pdf_qna.png')
 




        