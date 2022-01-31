import pandas.io.sql as sqlio
import numpy as np
from datetime import datetime
import boto3
import pandas as pd
from boto3.dynamodb.conditions import Key, Attr
from  datetime import datetime
import datetime as dt
import math
from sklearn.preprocessing import MinMaxScaler
import itertools
import yaml
import ast
from ast import literal_eval
import pymysql


#THE BELOW FUNCTION GETS THE DATA OF A SPECIFIED SMAC AND TIMESTAMP FOR GIVEN A TIME INTERVAL
def fetch_ddb_data(smac, intial_condition_time_epoch , max_temp, min_temp, config_file = "config.yaml", hour =1):
    
    config = yaml.load(open(config_file), Loader=yaml.FullLoader)
    dynamo_db = config['dynamodb']
    dynamodb = boto3.resource('dynamodb',
                          aws_access_key_id= dynamo_db['aws_access_key_id'],
                                  aws_secret_access_key= dynamo_db['aws_secret_access_key'],
                          region_name= dynamo_db['region_name'])
    
    
    table = dynamodb.Table('prod_sensor_reading')
    
    initial_condition_timestamp = dt.datetime.fromtimestamp(intial_condition_time_epoch / 1000)
    
    start_time_epoch = int(((initial_condition_timestamp - dt.timedelta(hours = 1)) - dt.datetime(1970, 1, 1)).total_seconds()* 1000)
    
#     end_time_epoch = int(((initial_condition_timestamp + dt.timedelta(minutes = window_length)) - dt.datetime(1970, 1, 1)).total_seconds()* 1000)
    
    result = table.query(
        ProjectionExpression=  'smac,#reading_timestamp,#air_temp ,agg', 
        ExpressionAttributeNames = {'#air_temp': 'temp', '#reading_timestamp': 'timestamp'},
        KeyConditionExpression= Key('smac').eq(smac) 
        & Key('timestamp').gte(start_time_epoch))
    
    df = parse_ddb_data(result, max_temp, min_temp)
    return(df)


#PARSES THE DATA RETURNED FROM DYNAMO-DB.
def parse_ddb_data(result, max_temp, min_temp):
   
    
    df = pd.DataFrame(result['Items'])
    df["agg"] = df["agg"].apply(lambda x: ast.literal_eval(x))
    df['prod_temp']= df['agg'].apply(pd.Series)['agg0']
    df = df.drop("agg",axis = 1)
    
    df["temp"] = df["temp"].astype(float)
    df["prod_temp"] = df["prod_temp"].astype(float)
    
#     df["temp"] = df["temp"].astype(float).apply(lambda x: np.round((x / 10.0 * 9.0/5.0 - 459.67) * 10) / 10)
#     df["prod_temp"] = df["prod_temp"].astype(float).apply(lambda x: np.round((x / 10.0 * 9.0/5.0 - 459.67) * 10) / 10)
    
    df["timestamp"] =  df["timestamp"].astype(float).apply(lambda x: dt.datetime.fromtimestamp(x / 1000))
    
    df = df.rename(columns={"temp":"air_temp", "timestamp": 'reading_timestamp'})
    df = df[['smac','air_temp','reading_timestamp', 'prod_temp']]
    
    df['max_temp_limit'] = max_temp
    df['min_temp_limit'] = min_temp
    df['event_id'] = 1
    
    
    return(df)
      


# THE BELOW FUNCTION CONVERTS K*10 READINGS TO FARENHEIT/CELCIUS
def convert_temp(df, columns = ['air_temp', 'prod_temp']):
    
    df  = df.sort_values(by = ['event_id', 'reading_timestamp'], ignore_index= True)
    
    for i in columns:
        temp = []
        for j in df[i]:
            temp.append(np.round((j / 10.0 * 9.0/5.0 - 459.67) * 10) / 10)
        df[i] = temp
     
    return df 
 
    
    
#THE BELOW FUNCTION CALCULATES AIR AND PRODUCT TEMP SEVERITY  
def calc_temp_severity (df, column_name = 'prod_temp'):
    
    df  = df.sort_values(by = ['event_id', 'reading_timestamp'], ignore_index= True)
    
    temp_severity = []
    air_temp_current  = 0
    air_temp_previous = 0
    datetimeFormat = '%Y-%m-%d %H:%M:%S.%f'
    count1 = 0
    value = 0
    
    df["reading_timestamp"] = pd.to_datetime(df['reading_timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S.%f'))
    
    for i in df["event_id"].unique():
        
        temp = df[df["event_id"] == i]
        bit = 1

        max_temp_limit = list(temp["max_temp_limit"])[0]
        min_temp_limit = list(temp["min_temp_limit"])[0]

        for j in temp.index:
        
            value = 0
            flag= 0
            count1  = count1 + 1
            air_temp_current = temp.loc[j][column_name]


            
            if (air_temp_current > max_temp_limit):
                
                    
                if (bit == 1):
                    
                    if (len(temp_severity) != 0):
                        value = temp_severity[len(temp_severity)-1] + ((300000 / 1000) * (air_temp_current - max_temp_limit))
                    else:
                        value = (300000 / 1000) * (air_temp_current - max_temp_limit)
                        
                else:
                    
                    
                    air_temp_previous = temp.loc[j-1][column_name]
                    
                    if (air_temp_previous <= max_temp_limit and air_temp_current >= min_temp_limit):

                        value = 0
                        flag = 1

                    if (air_temp_previous >= max_temp_limit and air_temp_current <= min_temp_limit):

                        value = 0
                        flag = 1


                    if (flag!= 1):


                        temp_previous = df.loc[j-1]
                        air_temp_previous = temp_previous[column_name]


                        current_time = temp.loc[j]["reading_timestamp"]
                        previous_time = temp.loc[j-1]["reading_timestamp"]


                        timeSpreadInMills = (current_time - previous_time).seconds * 1000

                        value  = temp_severity[len(temp_severity)-1] + ((timeSpreadInMills / 1000) * (air_temp_current - max_temp_limit))

            else:
                value = 0

            temp_severity.append(value)
            bit = 0

    return (temp_severity)
 
    
    
# THE BELOW FUNCTION CALCULATES THE SLOPE OF AIR AND PRODUCT TEMPERATURES  
def calc_slope(df):
    
    
    df  = df.sort_values(by = ['event_id', 'reading_timestamp'], ignore_index= True)
    
    df["slope_of_airtemp"] = 0
    df["slope_of_prodtemp"] = 0
    
    for i in list(df["event_id"].unique()):
        x = df[df["event_id"] == i].reset_index(drop = True) [["reading_timestamp", "air_temp"]]
        y = df[df["event_id"] == i].reset_index(drop = True) [["reading_timestamp", "prod_temp"]]
        slope_of_airtemp  = arctan_of_slope(x,"air_temp")
        slope_of_prodtemp = arctan_of_slope(y,"prod_temp")
        
        df["slope_of_airtemp"][df.event_id == i] = slope_of_airtemp
        df["slope_of_prodtemp"][df.event_id == i] = slope_of_prodtemp
    
    df  = df.sort_values(by = ['event_id', 'reading_timestamp'], ignore_index= True)
    return(df)



# THE BELOW FUNCTION CALCULATES THE SLOPE VALUE WHEH CALLED FOR A PARTICLUAR EVENT ID AND RETURNS THE TEMP_SEVERITY AS A LIST
def arctan_of_slope(df, column_name):
    
    diff_in_minutes = 0
    slope_of_airtemp = []
    
    reading_timestamp = list(df["reading_timestamp"])
    air_temp = list(df[column_name])
    
    for i in range(len(df)):
        if i == 0 :
            slope_of_airtemp.append(0)
        else: 
            
            if(i <= len(reading_timestamp)):

                diff_in_minutes = ((reading_timestamp[i] - reading_timestamp[i-1]).total_seconds()) / 60
                slope_of_airtemp.append( math.atan((air_temp[i] - air_temp[i-1]) / (diff_in_minutes)) )

    return(slope_of_airtemp)



# THE BELOW FUNCTION CALCULATES THE DIFFERENCES OF DIFFERENT COLUMNS AND RETURNS THEM AS COLUMNS OF A PASSED DERIVATIVES
def derive_differences (df):
    
    df["air_prod_temp_diff"] = df["air_temp"] - df["prod_temp"]
    df["air_max_temp_diff"] = df["air_temp"] - df["max_temp_limit"]
    df["prod_max_temp_diff"] = df["prod_temp"] - df["max_temp_limit"]
    
    return(df)



#THE BELOW CODE FETCHES SENSOR READINGS BETWEEN TWO TIMESTEMPS
def fetch_readings (start_time, end_time, smac, con):

    sql = "select   \
    r.smac as smac,\
    r.timestamp as reading_timestamp,\
    r.temp as air_temp,\
    r.agg0 as prod_temp,\
    s.max_safe_temp as max_temp_limit,\
    s.min_safe_temp as min_temp_limit, \
    r.netnum as netnum \
    from \
    reading r, sensor s \
    where \
    r.smac = "+str(smac)+" \
    and r.timestamp >='"+str(start_time)+"' \
    and r.timestamp <='"+str(end_time)+"' \
    and r.smac  = s.smac"
        
    readings = sqlio.read_sql_query(sql, con)
    
    readings['event_id'] = 0
    readings.sort_values(by = ['event_id', 'reading_timestamp'], ignore_index = True)
    
    return(readings)

    
#THE BELOW FUNCTION CALCULATES AVERAGES FOR PRODUCT AND AIR TEMPERATURE
def moving_averages (df):

    mvg_prod = []
    mvg_air = []
    
    for i in df["event_id"].unique():

        temp = df[df["event_id"] == i]
   
        mvg_prod.append(list(temp["prod_temp"].rolling(window=3).mean()))
        mvg_air.append(list(temp["air_temp"].rolling(window=3).mean()))  

    mvg_prod = list(itertools.chain(*mvg_prod))
    mvg_air = list(itertools.chain(*mvg_air))
    
    df["moving_average_air"] = mvg_air
    df["moving_average_prod"] = mvg_prod
    
    df[["moving_average_air", "moving_average_prod"]] = df[["moving_average_air", "moving_average_prod"]].fillna(0)
    
    return(df)


#THE BELOW FUNCTON CREATES A NEW DATA POINT BETWEEN TWO DATAPOINTS BY TAKING THEIR MEAN  
def create_mean_datapoint (df, i):
    

   
    
    new_data_points = []
    
    new_air_temp = (df.loc[i]["air_temp"] + df.loc[i+1]["air_temp"])/2
    new_prod_temp = (df.loc[i]["prod_temp"] + df.loc[i+1]["prod_temp"])/2
    
    five_minutes= dt.timedelta(minutes = 5)
    new_reading_timestamp = df["reading_timestamp"].loc[i] +  five_minutes
    
    return (df.loc[i]["smac"],  new_reading_timestamp, new_air_temp, new_prod_temp, df.loc[i]["max_temp_limit"], df.loc[i]["min_temp_limit"], df.loc[i]["event_id"], df.loc["start_of_event"])
   
    
    
#THE BELOW FUNCTION FILLS THE MISSING VALUES IN THE DATA BY FILLING MISSING DATA POINTS WITH MEAN VALUES 
def fill_missing_values(df):
    

    df = df.sort_values(by  = ["event_id","reading_timestamp"], ignore_index = True)
    
    datetimeFormat = '%Y-%m-%d %H:%M:%S.%f'
    
    
    df['reading_timestamp']=pd.to_datetime(df['reading_timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S.%f'))

    count = 0
    
    for k in df["event_id"].unique():
        
        temp = df[df["event_id"] == k].sort_values(by = "reading_timestamp", ignore_index = True)
        i=0
        
        while i in range(len(temp)):
            
            if (i+1 < len(temp)):
                
                


                diff_in_seconds = (temp["reading_timestamp"].loc[i+1] - temp["reading_timestamp"].loc[i]).seconds 

                

                if (diff_in_seconds >= 600):
                    count = count +1
                    mean_data_point  = create_mean_datapoint(temp, i)
                    
                    
                    index = df.index[(df["event_id"] == k) & (df['reading_timestamp'] == temp["reading_timestamp"].loc[i])]
                    index = int(index[0])
                   
                    line = pd.DataFrame([list(mean_data_point)], columns = list(df.columns), index=[index+1])
                    df = pd.concat([df.iloc[:index], line, df.iloc[index:]]).reset_index(drop=True)
                    
                    
                    df = df.sort_index().reset_index(drop=True)  
                    
                    
                    
                    df = df.sort_values(by = ["event_id","reading_timestamp"], ignore_index=True)
                    
                temp = df[df["event_id"] == k].sort_values(by = "reading_timestamp", ignore_index = True)
        
            i= i+1

        
        
    df = df.sort_index().reset_index(drop=True)  
    df = df.sort_values(by = ["event_id","reading_timestamp"], ignore_index=True)
    
    return(df)
            
# HE BELOW FUNCTION DROPS GIVEN COLUMNS OF A DATA FRAME AND RETURNS THE DATA FRAME
def column_drop(df, drop):
    
    df = df.drop(drop,axis =1)
    return(df)


def convert_temp_severity(pred_value):
     
        return int(10*(5/9)*360* pred_value * 10)
    

# THE BELOW FUNCTION IS USED TO DERIVE ALL THE FEATURES GIVEN DATA.
def derive_features(df):
      

    df.drop_duplicates(inplace = True ,subset = ["event_id", "reading_timestamp" ])
    
    df["reading_timestamp"] = pd.to_datetime(df['reading_timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S.%f'))
    
    df = df.sort_values(by = ["event_id", "reading_timestamp"])
    
    
    df = convert_temp(df)
   
    df = derive_differences(df)
    
    df["air_temp_severity"] = calc_temp_severity(df, "air_temp")
    df["prod_temp_severity"] = calc_temp_severity(df, "prod_temp")
    
    df["air_temp_severity"] = df["air_temp_severity"].apply(lambda x: np.round((x / 10) * (9 / 5) / 60 / 6) / 10)
    df["prod_temp_severity"] = df["prod_temp_severity"].apply(lambda x: np.round((x / 10) * (9 / 5) / 60 / 6) / 10)
    
    df = calc_slope(df)
    df = moving_averages(df)
    
    return(df)
    

# THE BELOW FUNCTION IS CONVERTS DATAFRAME TO A NUMPY ARRAY AND RESHAPES 2D DATA TO 3D DATA
def prepare_model_ready_data(df, drop_columns = ['smac', 'reading_timestamp', 'event_id']):
    
    
    model_input_data =  df.drop(drop_columns, axis = 1)
    model_input_array = model_input_data.to_numpy()
    model_input_array = np.asarray(model_input_array).astype(np.float32)
    
    model_input_array = np.reshape(model_input_array, (1,np.shape(model_input_array)[0], np.shape(model_input_array)[1]))
    
    
    scaler = MinMaxScaler()
    scaler.fit(model_input_array[:, 1, :])
    model_input_array[:, 1, :] = scaler.transform(model_input_array[:, 1, :])
    
    
    
    return(model_input_array)




        
        
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
