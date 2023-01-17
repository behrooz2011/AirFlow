
import json
import pathlib

import requests
import airflow
import requests.exceptions as requests_exceptions
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

print("start...")
""" Instantiate a DAG object; this is the starting point of any workflow."""
dag = DAG(
    #The name of the DAG:
    dag_id="download_rocket_launches",
    #The date at which the DAG should first start running:
    # start_date=airflow.utils.dates.days_ago(14),
    # start_date=pendulum.today('UTC').add(days=14),

    start_date=datetime(2023, 1, 1, 15, 0),
    #At what interval the DAG should run:
    schedule_interval=None, #deprecated
#     schedule=None,
 )
""" Bash command to download the URL response with curl:"""
download_launches = BashOperator(
    #name of the task
    task_id="download_launches",
    bash_command="curl -o /tmp/launches.json -L 'https://ll.thespacedevs.com/2.0.0/launch/upcoming'",
    dag=dag,
)

"""A Python function will parse
the response and download
all rocket pictures: """
def _gget_pictures():
    print("_get picture started")
# Ensure directory exists
    pathlib.Path("/tmp/images").mkdir(parents=True, exist_ok=True)
# Download all pictures in launches.json
    with open("/tmp/launches.json") as f:
        launches = json.load(f)
        image_urls = [launch["image"] for launch in launches["results"]]
        for image_url in image_urls:
            try:
                print('try')
                response = requests.get(image_url)
                print("response: ",response)
                image_filename = image_url.split("/")[-1]
                target_file = f"/tmp/images/{image_filename}"
                with open(target_file, "wb") as f:
                    f.write(response.content)
                print(f"Downloaded {image_url} to {target_file}")
            except requests_exceptions.MissingSchema:
                print(f"{image_url} appears to be an invalid URL.")
            except requests_exceptions.ConnectionError:
                print(f"Could not connect to {image_url}.")

""" Call the Python function in the
DAG with a PythonOperator: """
get_pictures = PythonOperator(
    task_id="get_pictures",
    python_callable=_gget_pictures,
    dag=dag,
)

notify = BashOperator(
    task_id="notify",
    bash_command='echo "Duuuuuuuuuuuuuuuuuuuuuuuuuuuude There are now $(ls /tmp/images/ | wc -l) images."',
    dag=dag,
)
print('end...')
'''Set the order of
execution of tasks: '''
download_launches >> get_pictures >> notify
print(">>>>>")