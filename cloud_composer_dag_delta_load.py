# -*- coding: utf-8 -*-
# Commented out IPython magic to ensure Python compatibility.
# # Commented out IPython magic to ensure Python compatibility.
# %%bash
# pip install apache-airflow

# %%bash
# airflow initdb

# # start the web server, default port is 8080
# %%bash
# airflow webserver -p 8080

import datetime
import requests
import uuid
import pandas as pd
import time

import airflow
import redis
from airflow.operators import DummyOperator, PythonOperator, BranchPythonOperator, BashOperator
from airflow.contrib.operators.mlengine_operator import MLEngineTrainingOperator
from google.cloud import storage
from google.cloud import firestore
from datetime import datetime as ds
from datetime import datetime as ds

##Global vars
project_id  =   '<gcp_project_id>'

######DAG main code
default_args = {
    'owner': 'Composer - Arya KX',
    'depends_on_past': False,
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': datetime.timedelta(minutes=5),
    'start_date': airflow.utils.dates.days_ago(0)
}

##Operation -> Check If Key store file exists
def delta_file_check(**kwargs):
    client 		= storage.Client()
    bucket_kx 	        = "{}-arya_ml_storage".format(project_id)
    blobs 		= client.list_blobs(bucket_kx, prefix="kx_delta_load/")

    for ix, blob in enumerate(blobs):
        if ix > 0:
            return('load_delta_files')
        
    return('End')

def validate_and_load_file(filename):
    client 		= storage.Client()
    file 	        = "gs://{}-arya_ml_storage/{}".format(project_id, filename)

    try:
        df = pd.read_excel(file)
        df.fillna("",inplace=True)
        
        if excel_validation(df) == False:
            print("All records passed")
            excel_read(df)
        else:
            return('fail')
    except:
        return('fail')

def load_data(**kwargs):
    client 		= storage.Client()
    bucket_kx 	        = "{}-arya_ml_storage".format(project_id)
    blobs 		= client.list_blobs(bucket_kx, prefix="kx_delta_load/")

    for ix, blob in enumerate(blobs):
        if ix > 0:
            print("Loading file: " + blob.name)
            ret = validate_and_load_file(blob.name)
            if ret == 'fail':
                copy_to_failed(blob.name)
            else:
                copy_to_success(blob.name)

            delete_file(blob.name)

def delete_file(filename):
    storage_client = storage.Client()
    bucket_kx 	        = "{}-arya_ml_storage".format(project_id)
    
    source_bucket = storage_client.bucket(bucket_kx)    
    blob = source_bucket.blob(filename)

    blob.delete()

def copy_to_failed(filename):
    storage_client = storage.Client()
    bucket_kx 	        = "{}-arya_ml_storage".format(project_id)
    
    source_bucket = storage_client.bucket(bucket_kx)
    destination_bucket = storage_client.bucket(bucket_kx)
    
    source_blob = source_bucket.blob(filename)

    source_bucket.copy_blob(
        source_blob, destination_bucket, "kx_delta_load_failed_files/" + filename.split("/")[-1]
    )

def copy_to_success(filename):
    storage_client = storage.Client()
    bucket_kx 	        = "{}-arya_ml_storage".format(project_id)
    
    source_bucket = storage_client.bucket(bucket_kx)
    destination_bucket = storage_client.bucket(bucket_kx)
    
    source_blob = source_bucket.blob(filename)

    source_bucket.copy_blob(
        source_blob, destination_bucket, "kx_delta_load_success_files/" + filename.split("/")[-1]
    )
    

#function for validating the records
def record_validation(row):
    object_dev_max_length_check = [
    'fs-type_specific_fields-conversion_source_system',
    'fs-type_specific_fields-conversion_target_system',
    'fs-type_specific_fields-conversion_type',
    'fs-type_specific_fields-enhancement_impacted_transactions',
    'fs-type_specific_fields-enhancement_type',
    'fs-type_specific_fields-form_output_methods',
    'fs-type_specific_fields-form_type',
    'fs-type_specific_fields-report_processing_mode',
    'fs-type_specific_fields-report_type',
    'fs-type_specific_fields-workflow_trigger',
    'fs-type_specific_fields-workflow_type',
    'ts-type_specific_fields-conversion_source_system',
    'ts-type_specific_fields-conversion_target_system',
    'ts-type_specific_fields-conversion_type',
    'ts-type_specific_fields-enhancement_impacted_transactions',
    'ts-type_specific_fields-enhancement_type',
    'ts-type_specific_fields-form_output_methods',
    'ts-type_specific_fields-form_type',
    'ts-type_specific_fields-report_processing_mode',
    'ts-type_specific_fields-report_type',
    'ts-type_specific_fields-workflow_configuration',
    'ts-type_specific_fields-workflow_trigger',
    'ts-type_specific_fields-workflow_type',
    'ts-type_specific_fields-interface_impacted_transactions',
    'ts-type_specific_fields-interface_processing_type',
    'ts-type_specific_fields-interface_type',
    'ts-general_fields-design_approach'
    ] #possible values
    
    object_dev_complexity_list = ['Low' , 'Med' , 'High' , 'Very High'] #possible values
    type_list = ['R','I','C','E','F','W']
    flag = 0
    status = ""
    
    if not row["object_dev-complexity"] in object_dev_complexity_list:
        flag = 1
        status = "INVALID: object_dev-complexity " + row["object_dev-complexity"]
        
    if not row["object_dev-type"] in type_list:
        flag = 1
        status = status+" INVALID: object_dev-type " + row["object_dev-type"]
        
    if not row["fs-general_fields-type"] in type_list:
        flag = 1
        status = status+" INVALID: fs-general_fields-type " + row["fs-general_fields-type"]
        
    for col_lc in object_dev_max_length_check:
        if(len(row[col_lc]) > 200):
            flag = 1
            status = status+" INVALID: " + col_lc
        
    return(flag, status)


# In[8]:
def excel_validation(df):
    print("Running file validation...")
    failed = False
    rec = len(df)
    for i ,row in df.iterrows():
        f,s= record_validation(row) #function call to validate the records

        #Incorrect record
        if f != 0:
            failed = True
            print('Row {} has errors, please fix - {}'.format(str(i+2), s))
    
    return failed

#function for uploading the records from excel
def excel_read(df):
    rec = len(df)
    db = firestore.Client()
    for i ,row in df.iterrows():
        d = {}
        d["fs"] = {}
        d["fs"]["general_fields"] = {}
        d["fs"]["type_specific_fields"] ={}
        
        d["object_dev"] = {}
        
        d["ts"] = {}
        d["ts"]["general_fields"] = {}
        d["ts"]["type_specific_fields"] = {}
        
        d["fs"]["general_fields"]["assumptions"] = row["fs-general_fields-assumptions"]
        d["fs"]["general_fields"]["business_driver"] = row["fs-general_fields-business_driver"]
        d["fs"]["general_fields"]["dependencies"] = row["fs-general_fields-dependencies2"]
        d["fs"]["general_fields"]["description"] = row["fs-general_fields-description"]
        d["fs"]["general_fields"]["exception_handling"] = row["fs-general_fields-exception_handling"]
        d["fs"]["general_fields"]["requirements"] = row["fs-general_fields-requirements"]
        d["fs"]["general_fields"]["summary"] = row["fs-general_fields-summary"]
        d["fs"]["general_fields"]["type"] = row["fs-general_fields-type"]
        d["fs"]["general_fields"]["uuid"] = str(uuid.uuid4())
        
        d["fs"]["type_specific_fields"]["conversion_source_system"] = row["fs-type_specific_fields-conversion_source_system"]
        d["fs"]["type_specific_fields"]["conversion_target_system"] = row["fs-type_specific_fields-conversion_target_system"]
        d["fs"]["type_specific_fields"]["conversion_type"] = row["fs-type_specific_fields-conversion_type"]
        d["fs"]["type_specific_fields"]["enhancement_impacted_transactions"] = row["fs-type_specific_fields-enhancement_impacted_transactions"]
        d["fs"]["type_specific_fields"]["enhancement_type"] = row["fs-type_specific_fields-enhancement_type"]
        d["fs"]["type_specific_fields"]["form_output_methods"] = row["fs-type_specific_fields-form_output_methods"]
        d["fs"]["type_specific_fields"]["form_type"] = row["fs-type_specific_fields-form_type"]
        d["fs"]["type_specific_fields"]["report_processing_mode"] = row["fs-type_specific_fields-report_processing_mode"]
        d["fs"]["type_specific_fields"]["report_type"] = row["fs-type_specific_fields-report_type"]
        d["fs"]["type_specific_fields"]["uuid"] = str(uuid.uuid4())
        d["fs"]["type_specific_fields"]["workflow_trigger"] = row["fs-type_specific_fields-workflow_trigger"]
        d["fs"]["type_specific_fields"]["workflow_type"] = row["fs-type_specific_fields-workflow_type"]
        
        d["object_dev"]["complexity"] = row["object_dev-complexity"]
        d["object_dev"]["requirement"] = row["object_dev-requirement"]
        d["object_dev"]["system_id"] = "ARYA_KX_PROD"
        d["object_dev"]["type"] = row["object_dev-type"]
        d["object_dev"]["uuid"] = str(uuid.uuid4())
        
        d["timestamp"] = time.time()
        
        d["ts"]["general_fields"]["assumptions_dependencies"] = row["ts-general_fields-assumptions_dependencies"]
        d["ts"]["general_fields"]["description"] = row["ts-general_fields-description"]
        d["ts"]["general_fields"]["exception_handling"] = row["ts-general_fields-exception_handling"]
        d["ts"]["general_fields"]["security_requirements"] = row["ts-general_fields-security_requirements"]
        d["ts"]["general_fields"]["design_approach"] = row["ts-general_fields-design_approach"]
        d["ts"]["general_fields"]["summary"] = row["ts-general_fields-summary"]
        d["ts"]["general_fields"]["technical_flow"] = row["ts-general_fields-technical_flow"]
        d["ts"]["general_fields"]["type"] = row["ts-general_fields-type"]
        d["ts"]["general_fields"]["uuid"] = str(uuid.uuid4())
        
        d["ts"]["type_specific_fields"]["conversion_source_system"] = row["ts-type_specific_fields-conversion_source_system"]
        d["ts"]["type_specific_fields"]["conversion_target_system"] = row["ts-type_specific_fields-conversion_target_system"]
        d["ts"]["type_specific_fields"]["conversion_type"] = row["ts-type_specific_fields-conversion_type"]
        d["ts"]["type_specific_fields"]["enhancement_impacted_transactions"] = row["ts-type_specific_fields-enhancement_impacted_transactions"]
        d["ts"]["type_specific_fields"]["enhancement_type"] = row["ts-type_specific_fields-enhancement_type"]
        d["ts"]["type_specific_fields"]["form_output_methods"] = row["ts-type_specific_fields-form_output_methods"]
        d["ts"]["type_specific_fields"]["form_type"] = row["ts-type_specific_fields-form_type"]
        d["ts"]["type_specific_fields"]["interface_impacted_transactions"] = row["ts-type_specific_fields-interface_impacted_transactions"]
        d["ts"]["type_specific_fields"]["interface_processing_type"] = row["ts-type_specific_fields-interface_processing_type"]
        d["ts"]["type_specific_fields"]["interface_type"] = row["ts-type_specific_fields-interface_type"]
        d["ts"]["type_specific_fields"]["report_processing_mode"] = row["ts-type_specific_fields-report_processing_mode"]
        d["ts"]["type_specific_fields"]["report_type"] = row["ts-type_specific_fields-report_type"]
        d["ts"]["type_specific_fields"]["workflow_configuration"] = row["ts-type_specific_fields-workflow_configuration"]
        d["ts"]["type_specific_fields"]["workflow_trigger"] = row["ts-type_specific_fields-workflow_trigger"]
        d["ts"]["type_specific_fields"]["workflow_type"] = row["ts-type_specific_fields-workflow_type"]
        
        d["uuid_kx"] = str(uuid.uuid4())

        ##TUT/ FUT data:
        dict_tut = {}
        dict_fut = {}
        arr_tut = []
        arr_fut = []
        for ut_ in range(5):
            ut_ix = ut_ + 1

            ##TUT data
            if not row['tut-test_case' + str(ut_ix) + '-scenario'] == "":
                dict_tut = {}
                dict_tut['test_case'] = str(ut_ix)
                dict_tut['scenario'] = row['tut-test_case' + str(ut_ix) + '-scenario']
                dict_tut['expected_results'] = row['tut-test_case' + str(ut_ix) + '-expected_results']
                arr_tut.append(dict_tut)

            ##FUT data
            if not row['fut-test_case' + str(ut_ix) + '-scenario'] == "":
                dict_fut = {}
                dict_fut['test_case'] = str(ut_ix)
                dict_fut['scenario'] = row['fut-test_case' + str(ut_ix) + '-scenario']
                dict_fut['expected_results'] = row['fut-test_case' + str(ut_ix) + '-expected_results']
                arr_fut.append(dict_fut)
                
        
        d["ut"] = {"description":"","fut_test_cases":arr_tut,"tut_test_cases":arr_fut}
        
        name = d["object_dev"]["system_id"] + "_" + d["object_dev"]["uuid"] #document name to be uploaded on firestore

        f,s= record_validation(row) #function call to validate the records

        if f == 0:
            status = "VALID"
        else:
            status = s
            
        if status == "VALID":
            doc_ref = db.collection('cleansed').document(name)
            doc_ref.set(d)
            d.clear()
            print("Successfully uploaded record {} of {}".format(i+1, rec))
        else:
            df.at[i,'status'] = status
            print("Failed record {}, message - {}".format(i, status))

    return('success')

with airflow.DAG(
        'arya_kx_delta_load',
        'catchup=False',
        default_args=default_args,
        schedule_interval=datetime.timedelta(hours=12)) as dag:

    Start = DummyOperator(task_id='Start')

    is_new_delta_file = BranchPythonOperator(
                task_id='is_new_delta_file',
                provide_context=True,
                python_callable=delta_file_check,
                trigger_rule="all_done",
                dag=dag)

    load_delta_files = PythonOperator(
                task_id='load_delta_files',
                provide_context=True,
                python_callable=load_data,
                dag=dag)

    End = DummyOperator(task_id='End')

    Start >> is_new_delta_file
    is_new_delta_file >> [ load_delta_files, End ]
    load_delta_files >> End
