FROM public.ecr.aws/lambda/python:3.8

# Copy function code
COPY app.py ${LAMBDA_TASK_ROOT}
COPY Dynamic_feature_engineering.py ${LAMBDA_TASK_ROOT}
COPY model2_regression_testing_best1_without_mve_MSE.h5 ${LAMBDA_TASK_ROOT}
COPY config.yaml ${LAMBDA_TASK_ROOT}

# Install the function's dependencies using file requirements.txt
# from your project folder.

COPY requirements.txt  .
RUN  pip3 install -r requirements.txt --target "${LAMBDA_TASK_ROOT}" 

#ENTRYPOINT ["python"]
# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD ["app.lambda_handler"] 
