from logger  import logging 
import boto3 
from boto3.dynamodb.conditions import Key     
import Dynamic_feature_engineering as dfe
# import pandas as pd
# import numpy as np
import datetime as dt
 



def lambda_handler(event, context):
     
    responce = []
    logger = logging.getLogger('ml_pred') 
    keys= ['smac', 'time_epoch', 'max_temp', 'min_temp']
    error_message = ''
    status = "SUCCESS"
    
    for i in keys:
        if i not in event.keys():
            logger.info("'"+str(i)+"' NOT FOUND")
            message = "REQUEST BODY ERROR"
            status = "FAILED"
            return({"response_obj" : {"smac" : str(-1) , "pred": str(-1), "error_message" : error_message, "status" : status}})
    
    
    if "smac" in event.keys():
        
        if int(event['smac']) == 0:
            logger.info("SERVER TEST")
            return("TEST SUCCEDED ... SERVER RUNNING ....") 
        
        
    for i in event.values():
        
        if i == "" or i== " " :

            logger.info("Values Missing...")
            message = "REQUEST BODY ERROR"
            status = "FAILED"
            return({"response_obj" : {"smac" : str(-1) , "pred": str(-1), "error_message" : error_message, "status" : status}})

    smac = int(event['smac'])
    time_epoch = int(event['time_epoch'])
    

    logger.info("CALCULATING AVERAGE AND ALERT STATUS")
    
    
    alert_status = dfe.get_alert_decision(smac, time_epoch, 3, db = 'mysql')
    
    if (alert_status['alert_status'] == -1):
        
        logger.info("smac: "+str(smac)+" error: "+str(alert_status['error_message']))
        
        return({"response_obj" : {"smac":str(smac),"error_message" : str(alert_status['error_message']), "alert_status" : str(alert_status['alert_status'])}})
    
    
    
                
    
    avg =  alert_status['avg']
    

    
    logger.info("RESPONSE OBJECT RETURNING")
    
    logger.info("smac: "+ str(smac) +" error_message: " +str(alert_status['error_message']) + " status: "+str(status)+ " alert_status: "+str(alert_status['alert_status'])+ " avg : " +str(avg) +' trigger_0: ' + str(alert_status['threshold_array'][0])+ ' trigger_1 :'+ str(alert_status['threshold_array'][1]) + ' trigger_2 :' + str(alert_status['threshold_array'][2]))
    
    return({"response_obj" : {"smac" : str(smac) , "error_message" : str(alert_status['error_message']), "status" : str(status), "alert_status" : str(alert_status['alert_status']), "avg" : str(avg), 'trigger_0' : str(alert_status['threshold_array'][0]), 'trigger_1' : str(alert_status['threshold_array'][1]), 'trigger_2' : str(alert_status['threshold_array'][2])}})
    
