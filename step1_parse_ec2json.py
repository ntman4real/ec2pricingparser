from __future__ import print_function
import boto3, ujson, urllib
from datetime import datetime

s3 = boto3.resource('s3')
s3cl = boto3.client('s3')
awspriceURLtest = 'https://s3.amazonaws.com/awspricingspa/jsons/products-sm.json'
awspriceURLtestbig = 'https://s3.amazonaws.com/awspricingspa/jsons/aws_ec2.json'
awspriceURL = 'https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonEC2/current/index.json'
bucket='awspricingspa'
parsedproducts = 'jsons/parsed-products.json'
parsedterms = 'jsons/parsed-terms.json'

def parse_ec2first(event,context):
    startTime = datetime.now()
    urllib.urlretrieve(awspriceURL, '/tmp/temparse.json')
    proddicts = {}
    termsdicts = {}
    try:
        with open('/tmp/temparse.json') as json_file:
            data = ujson.load(json_file)
        for name, info in data['products'].items():
            proddicts.update({
                name : info,
            })
        with open('/tmp/parsed-products.json', "w") as f:
            ujson.dump(proddicts, f)

        for name, info in data['terms'].items():
            termsdicts.update({
                name : info,
            })
        with open('/tmp/parsed-terms.json', "w") as f:
            ujson.dump(termsdicts, f)
    finally:
        response1 = open('/tmp/parsed-products.json')
        response2 = open('/tmp/parsed-terms.json')
        products = response1.read()
        terms = response2.read()
        s3cl.put_object(Bucket=bucket, Key=parsedproducts, Body=products)
        s3cl.put_object(Bucket=bucket, Key=parsedterms, Body=terms)
    print(datetime.now() - startTime)
#parse_ec2prices()