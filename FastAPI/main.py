import uvicorn
import pandas as pd
from fastapi import Depends, FastAPI, HTTPException, status
from pydantic import BaseModel
from fastapi.responses import JSONResponse
import requests
import wget
import re
import os
from io import BytesIO
from pypdf import PdfReader
import gunicorn
import httpx
import json
import openai
import mysql.connector
import pymysql.cursors

from datetime import datetime, timedelta
from typing import Annotated, Optional

from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jose import JWTError, jwt
from passlib.context import CryptContext

from google.cloud.sql.connector import Connector
import sqlalchemy
import pymysql

# initialize Connector object
connector = Connector()
 
# function to return the database connection
def getconn() -> pymysql.connections.Connection:
    conn: pymysql.connections.Connection = connector.connect(
        "",
        "pymysql",
        user="",
        password="",
        db=""
    )
    return conn
 
# create connection pool
pool = sqlalchemy.create_engine(
    "mysql+pymysql://",
    creator=getconn,
)

SECRET_KEY = ""
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 300



def get_password_hash(password):
    return pwd_context.hash(password)


class Token(BaseModel):
    access_token: str
    token_type: str

class TokenData(BaseModel):
    username: Optional[str] = None

class User(BaseModel):
    username: str

class UserInDB(User):
    hashed_password: str

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")


app = FastAPI()


def verify_password(plain_password, hashed_password):
    return pwd_context.verify(plain_password, hashed_password)


def get_password_hash(password):
    return pwd_context.hash(password)


def get_user(db_connection, username: str):
    with db_connection.cursor(pymysql.cursors.DictCursor) as cursor:
        cursor.execute("SELECT user, password FROM users WHERE user= %s", (username,))
        user_record = cursor.fetchone()

    if user_record:
        # Ensure that the values are strings
        user_in_db = UserInDB(
            username=str(user_record['user']),
            hashed_password=str(user_record['password'])
        )
        return user_in_db
    

def authenticate_user(username: str, password: str):
    db_connection = getconn()
    user = get_user(db_connection, username)
    db_connection.close()  # Make sure to close the connection
    if not user:
        return False
    if not verify_password(password, user.hashed_password):
        return False
    return user



def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt



async def get_current_user(token: str = Depends(oauth2_scheme)):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
        db_connection = getconn()
        user = get_user(db_connection, username)
        db_connection.close()
        if user is None:
            raise credentials_exception
        return user
    except JWTError:
        raise credentials_exception


async def get_current_active_user(
    current_user: Annotated[User, Depends(get_current_user)]
):
    if current_user.disabled:
        raise HTTPException(status_code=400, detail="Inactive user")
    return current_user


@app.post("/users/register", response_model=User)
async def create_user_signup(user: UserInDB):
    connection = getconn()
    try:
        cursor = connection.cursor()
        # Check if user already exists
        cursor.execute("SELECT * FROM users WHERE user = %s", (user.username,))
        existing_user = cursor.fetchone()
        if existing_user:
            raise HTTPException(status_code=400, detail="Username already registered")
        
        # Hash the user's password and create new user
        hashed_password = get_password_hash(user.hashed_password)
        cursor.execute(
            "INSERT INTO users (user, password, disabled) VALUES (%s, %s, %s)",
            (user.username, hashed_password, False)
        )
        connection.commit()
    except mysql.connector.Error as e:
        connection.rollback()
        raise HTTPException(status_code=500, detail="An error occurred while registering the user")
    finally:
        cursor.close()
        connection.close()

    # Return the user information excluding the hashed_password
    return User(username=user.username)




'''@app.post("/token", response_model=Token)
async def login_for_access_token(
    form_data: Annotated[OAuth2PasswordRequestForm, Depends()]
):
    user = authenticate_user(fake_users_db, form_data.username, form_data.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": user.username}, expires_delta=access_token_expires
    )
    return {"access_token": access_token, "token_type": "bearer"}'''


@app.post("/token", response_model=Token)
async def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends()):
    db_connection = getconn()
    user = authenticate_user(form_data.username, form_data.password)
    db_connection.close()
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    # If authentication is successful, we return a Token model.
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": user.username}, expires_delta=access_token_expires
    )
    return {"access_token": access_token, "token_type": "bearer"}


@app.get("/users/me/", response_model=User)
async def read_users_me(
    current_user: Annotated[User, Depends(get_current_active_user)]
):
    return current_user


@app.get("/users/me/items/")
async def read_own_items(
    current_user: Annotated[User, Depends(get_current_active_user)]
):
    return [{"item_id": "Foo", "owner": current_user.username}]


            
   
if __name__ == "__main__":

    uvicorn.run(app, host="0.0.0.0", port=8000)