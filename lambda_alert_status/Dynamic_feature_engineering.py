import pandas.io.sql as sqlio
import numpy as np
from datetime import datetime
import boto3
import pandas as pd
from boto3.dynamodb.conditions import Key, Attr
from  datetime import datetime
import datetime as dt
import math
import yaml
import ast
from ast import literal_eval
import pymysql
from logger  import logging 

logger = logging.getLogger('ml_pred')




def convert_temp_severity(pred_value):
     
        return int(10*(5/9)*360* pred_value * 10)
    

    
    
def k_to_f (x):
    return(np.round(( x/ 10) * (9 / 5) / 60 / 6) / 10)



def get_prediction_average(connection, smac, idt, window):

    
    pred_temp_severity = []
    
    query = "SELECT mp.smac, mlh.ml_alert_predictionid, mlh.prediction_requested_timestamp, mlh.prediction_response_timestamp, mlh.timestamp, mlh.initial_condition_timestamp, mlh.predicted_temp_severity, mlh.current_temp_severity  from ml_alert_prediction_history mlh, ml_alert_prediction mp  where mlh.ml_alert_predictionid = mp.id and smac = "+str(smac)+" and mp.initial_condition_timestamp = '"+str(idt)+"' order by mlh.prediction_response_timestamp"

    
    ml_pred_history = pd.read_sql(query, connection)
    
    
    for i in ml_pred_history['predicted_temp_severity'].tail(window):
        
        pred_temp_severity.append(i)
        
        
    logger.info("NUMBER OF PREDICTIONS: "+str(len(pred_temp_severity)))
    
    if (len(pred_temp_severity) >= window):

        avg = weighted_average(pred_temp_severity)

    else:

        avg = -1

    return(avg)



def get_alert_threshold (smac ,connection):
    
    
    
    sensor_id = pd.read_sql("select smac, id, tags  from sensor where smac = "+str(smac) , connection)
    stm = pd.read_sql("select * from sensor where id = "+str(sensor_id['id'].values[0]) , connection)
    als = pd.read_sql("select * from alert_setting", connection)
    
            
        
        
    if len(als[als['tagid'].isin(sensor_id[sensor_id['id'] == sensor_id['id'].values[0]]['tags'])]) > 0:

        als_max_priority_index = als[als['tagid'].isin(sensor_id[sensor_id['id']== sensor_id['id'].values[0]]['tags'])]['priority'].idxmax()
    
    

            
    als_setting = als.loc[als_max_priority_index][['alert_trigger_method', 'minutes_to_trigger0', 'minutes_to_trigger1', 'minutes_to_trigger2', 'unit_seconds_exposure_to_trigger0', 'unit_seconds_exposure_to_trigger1', 'unit_seconds_exposure_to_trigger2']]
    
    if (als_setting['alert_trigger_method'] == 2):
        

        
        return(als_setting[['unit_seconds_exposure_to_trigger0', 'unit_seconds_exposure_to_trigger1', 'unit_seconds_exposure_to_trigger2']])
        
    return(-1)
    
    
    
def weighted_average (x):
    
    x = list(x)
    w_avg = 0
    
    for i in range(len(x)):
        
        w_avg = w_avg + ((i+1) * x[i])
        
    return (w_avg / sum(list(range(1,len(x)+1))))


def get_alert_comm (avg_prediction, alert_threshold):
       
    logger.info("AVERAGE... "+ str(k_to_f(avg_prediction)))
    
    comm_type = []
    columns = ['unit_seconds_exposure_to_trigger0', 'unit_seconds_exposure_to_trigger1', 'unit_seconds_exposure_to_trigger2']
    
    for i in columns:
        
    
        if(alert_threshold[i]!= 0):

            threshold = k_to_f(alert_threshold[i])

            logger.info("Threshold..."+str(threshold * 1.5))
            
            if (k_to_f(avg_prediction) >= (threshold * 1.5)) :
                
                comm_type.append(1)

            else:

                comm_type.append(0)

        else:
            comm_type.append(-1)

    
    return(comm_type)
    

def get_alert_decision (smac, idt, window, db ,config_file = "config.yaml"):
    
    initial_condition_timestamp = dt.datetime.fromtimestamp(idt / 1000)
    config = yaml.load(open(config_file), Loader=yaml.FullLoader)
    mysql = config[db]
    
    
    connection = pymysql.connect(host= mysql['host'],
                                 user= mysql['user'],
                             password= mysql['password'],
                             database=mysql['database'],
                             port = 3306
                            )
    
    avg_prediction  = get_prediction_average(connection, smac, initial_condition_timestamp, window)

    alert_threshold = get_alert_threshold(smac, connection)
    
    
    
    if (avg_prediction == -1 ):
            
            return({'error_message' : 'NOT ENOUGH PREDICTIONS',  'alert_status' : -1, 'avg' : -1})
        
    if (len(alert_threshold) == 0):
            
            return({'error_message' : 'NO EXPOSURE BASED ALERT SETTING', 'alert_status' : -1, 'avg' : -1})
    
    
    comm_sent_array = get_alert_comm(avg_prediction, alert_threshold)
    
    
    if 1 in comm_sent_array:
           
        return({'error_message' : '', 'alert_status' : '1', 'threshold_array' : comm_sent_array , 'avg' : int(avg_prediction)})
   
    else: 
        
        return({'error_message' : '', 'alert_status' : '0', 'threshold_array' : comm_sent_array , 'avg' : int(avg_prediction) })
    
    

    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
