FROM python:3.9

RUN pip install pipenv
# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . ./
# Install any needed packages specified in requirements.txt
# RUN pip install --trusted-host pypi.python.org -r requirements.txt

RUN pipenv install --system --deploy --ignore-pipfile

# Make port 80 available to the world outside this container
EXPOSE 8080

# Define environment variable
# ENV NAME World

# Run app.py when the container launches
CMD ["airflow", "run", "sandbox.py", "--server.port", "8080"]