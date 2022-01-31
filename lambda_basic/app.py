from logger  import logging 
import boto3 
from boto3.dynamodb.conditions import Key     
import pandas.io.sql as sqlio
import Dynamic_feature_engineering as dfe
import pandas as pd
import numpy as np
from tensorflow.keras.models import load_model
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
            error_message = "REQUEST BODY ERROR"
            status = "FAILED"
            return({"response_obj" : {"smac" : str(-1) , "pred": str(-1), "error_message" : error_message, "status" : status}})
    
    
    if "smac" in event.keys():
        
        if int(event['smac']) == 0:
            logger.info("SERVER TEST")
            return("TEST SUCCEDED ... SERVER RUNNING ....") 
        
        
    for i in event.values():
        
        if i == "" or i== " " :

            logger.info("MISSING VALUES...")
            error_message = "REQUEST BODY ERROR"
            status = "FAILED"
            return({"response_obj" : {"smac" : str(-1) , "pred": str(-1), "error_message" : error_message, "status" : status}})

    smac = int(event['smac'])
    time_epoch = int(event['time_epoch'])
    max_temp = float(event['max_temp'])
    min_temp = float(event['min_temp'])
    
        
    logger.info("REQUESTED PREDICTIONS")

    df = dfe.fetch_ddb_data(smac, time_epoch, max_temp, min_temp)
    logger.info("FETCHDED DATA")
    
    if (len(df) < 5):
        logger.warning("NOT ENOUGH DATA FOR PREDICTION")
        error_message = "NOT ENOUGH DATA FOR PREDICTION"
        status = "FAILED"
        return({"response_obj" : {"smac" : str(smac) , "pred": str(-1), "error_message" : error_message, "status" : status}})
    
    test_data = dfe.derive_features(df)        
    logger.info("FEATURES DERIVED")
    
               
    test_data = dfe.column_drop(test_data, ['air_temp_severity', 'moving_average_prod', 'moving_average_air'])
    logger.info("DROPPED COLUMNS : 'air_temp_severity', 'moving_average_prod', 'moving_average_air' ")
        
    test_array = dfe.prepare_model_ready_data(test_data)

    logger.info("MODEL READY DATA FETCHED")

    test_model1 = load_model('model2_regression_testing_best1_without_mve_MSE.h5')

    pred = test_model1.predict(test_array)
    
    logger.info("RESPONSE : smac :"+str(smac)+" pred: "+str(pred[0][0]))
    
    pred_value  = dfe.convert_temp_severity(pred[0][0])
    
    
    
    
    logger.info("PREDICTION DONE... RESPONSE OBJECT RETURNING")
    
                
    logger.info("RESPONSE : smac :"+str(smac)+" pred: "+str(pred_value))
    
    
    return({"response_obj" : {"smac" : str(smac) , "pred": str(pred_value), "error_message" : error_message}})
    
