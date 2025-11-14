FROM public.ecr.aws/lambda/python:3.9
COPY app.py .
RUN pip install boto3
CMD ["python3", "app.py"]