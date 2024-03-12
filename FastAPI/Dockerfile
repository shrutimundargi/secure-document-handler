FROM python:3.9

RUN pip install pipenv
# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY  main.py ./
COPY form_qa.json ./
COPY  Pipfile Pipfile.lock ./
# Install any needed packages specified in requirements.txt
# RUN pip install --trusted-host pypi.python.org -r requirements.txt

RUN pipenv install --system --deploy --ignore-pipfile

# Make port 80 available to the world outside this container
EXPOSE 8000

# Define environment variable
# ENV NAME World

# Run app.py when the container launches
CMD ["gunicorn", "-w", "4", "-k", "uvicorn.workers.UvicornWorker", "--bind", "0.0.0.0:8000",  "main:app"]