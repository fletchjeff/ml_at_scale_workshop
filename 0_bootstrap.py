## Part 0: Bootstrap File
# You need to at the start of the project. It will install the requirements, creates the 
# STORAGE environment variable and copy the data from 
# raw/WA_Fn-UseC_-Telco-Customer-Churn-.csv into /datalake/data/churn of the STORAGE 
# location.

# The STORAGE environment variable is the Cloud Storage location used by the DataLake 
# to store hive data. On AWS it will s3a://[something], on Azure it will be 
# abfs://[something] and on CDSW cluster, it will be hdfs://[something]

# Install the requirements
!pip3 install git+https://github.com/fastforwardlabs/cmlbootstrap#egg=cmlbootstrap
!pip3 install flask
!Rscript -e "install.packages('dplyr')"
!Rscript -e "install.packages('tibble')"
!Rscript -e "install.packages('sparklyr')"
!Rscript -e "install.packages(c('psych','ggthemes','leaflet'))"

# Create the directories and upload data
from cmlbootstrap import CMLBootstrap
import os
import xml.etree.ElementTree as ET

# Set the setup variables needed by CMLBootstrap
HOST = os.getenv("CDSW_API_URL").split(
    ":")[0] + "://" + os.getenv("CDSW_DOMAIN")
USERNAME = os.getenv("CDSW_PROJECT_URL").split(
    "/")[6]  # args.username  # "vdibia"
API_KEY = os.getenv("CDSW_API_KEY") 
PROJECT_NAME = os.getenv("CDSW_PROJECT")  

# Instantiate API Wrapper
cml = CMLBootstrap(HOST, USERNAME, API_KEY, PROJECT_NAME)

# Set the STORAGE environment variable
try : 
  storage=os.environ["STORAGE"]
except:
  if os.path.exists("/etc/hadoop/conf/hive-site.xml"):
    tree = ET.parse('/etc/hadoop/conf/hive-site.xml')
    root = tree.getroot()
    for prop in root.findall('property'):
      if prop.find('name').text == "hive.metastore.warehouse.dir":
        storage = prop.find('value').text.split("/")[0] + "//" + prop.find('value').text.split("/")[2]
  else:
    storage = "/user/" + os.getenv("HADOOP_USER_NAME")
  storage_environment_params = {"STORAGE":storage}
  storage_environment = cml.create_environment_variable(storage_environment_params)
  os.environ["STORAGE"] = storage

# Upload the data to the cloud storage
!hdfs dfs -mkdir -p $STORAGE/datalake
!hdfs dfs -mkdir -p $STORAGE/datalake/data
!hdfs dfs -mkdir -p $STORAGE/datalake/data/airlines
!hdfs dfs -mkdir -p $STORAGE/datalake/data/airlines/csv
!for i in $(seq 2009 2018); do curl https://cdp-demo-data.s3-us-west-2.amazonaws.com/$i.csv | hadoop fs -put - $STORAGE/datalake/data/airlines/csv/$i.csv; done
!curl https://cdp-demo-data.s3-us-west-2.amazonaws.com/airports.csv | hadoop fs -put - $STORAGE/datalake/data/airlines/airports.csv  
