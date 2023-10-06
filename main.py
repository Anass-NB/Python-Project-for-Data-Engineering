import glob                         # this module helps in selecting files 
import pandas as pd                 # this module helps in processing CSV files
from datetime import datetime

tmpfile    = "temp.tmp"               # file used to store all extracted data
targetfile = "transformed_data.csv"   # file where transformed data is stored
logfile    = "logfile.txt"            # all event logs will be stored in this file

def extract_csv(csv_file_to_extract):
  df = pd.read_csv(csv_file_to_extract)
  return df


def extract_json(json_file_to_extract):
  df = pd.read_json(json_file_to_extract,lines=True)
  return df


def extract():
  extracted_data = pd.DataFrame(columns=['name','height','weight']) # create an empty data frame to hold extracted data  return extracted_data
    #process all csv files

  for csvfile in  glob.glob("*.csv"):
    extracted_data = pd.concat([extracted_data, extract_csv(csvfile)],ignore_index=True)
    # extracted_data.append(extract_csv(csvfile))
    # As of pandas 2.0, append (previously deprecated) was removed.You need to use concat instead

    #process all json files
  for jsonfile in  glob.glob("*.json"):
    extracted_data = pd.concat([extracted_data, extract_json(jsonfile)],ignore_index=True)

  return extracted_data

def transform(data):
  #Convert height which is in inches to millimeter
  #Convert the datatype of the column into float
  #data.height = data.height.astype(float)
  #Convert inches to meters and round off to two decimals(one inch is 0.0254 meters)
  data['height'] = round(data.height * 0.0254,2)
  
  #Convert weight which is in pounds to kilograms
  #Convert the datatype of the column into float
  #data.weight = data.weight.astype(float)
  #Convert pounds to kilograms and round off to two decimals(one pound is 0.45359237 kilograms)
  data['weight'] = round(data.weight * 0.45359237,2)
  return data

def load(targetfile,data_to_load):
  data_to_load.to_csv(targetfile) 


def log(message):
  timestamp_format = '%H:%M:%S-%h-%d-%Y' #Hour-Minute-Second-MonthName-Day-Year
  now = datetime.now() # get current timestamp
  timestamp = now.strftime(timestamp_format)
  with open("ETL_exercice_logfile.txt","a") as f:
      f.write(timestamp + ',' + message + '\n') 


log("ETL Job Started")

log("Extract phase Started")
extracted_data = extract()
log("Extract phase Ended")

log("Transform phase Started")
transformed_data = transform(extracted_data)
log("Transform phase Ended")

log("Load phase Started")
load(targetfile,transformed_data)
log("Load phase Ended")

log("ETL Job Ended")