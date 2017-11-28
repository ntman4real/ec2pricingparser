from __future__ import print_function
import boto3, ujson, urllib
ec2_productsURL = 'https://s3.amazonaws.com/awspricingspa/jsons/ec2-products.json'
tmpproductsfile = '/tmp/tempproducts.json'
urllib.urlretrieve(ec2_productsURL, tmpproductsfile)
with open(tmpproductsfile) as json_file1:
    prodf = ujson.load(json_file1)
myTable='ec2_pricing'
dynamodb = boto3.resource(service_name='dynamodb')
table = dynamodb.Table(myTable)
def ddb_loadproducts(event,context):
    for item in prodf:
        name = item['name']
        type = item['type']
        family = item['family']
        Network = item['Network']
        OS = item['OS']
        ens = item['ens']
        storage = item['storage']
        clockSpeed = item['clockSpeed']
        mem = item['mem']
        cpu = item['cpu']
        ebsOptimized = item['ebsOptimized']
        series = item['series']
        regions = item['regions']
        regiongrp = item['regiongrp']
        region_name = item['region_name']
        table.put_item(
                   Item={
                'name': name,
               'type': type,
               'family': family,
               'Network': Network,
               'OS' : OS,
               'ens' : ens,
               'ebsOptimized' : ebsOptimized,
               'storage' : storage,
               'clockSpeed' : clockSpeed,
               'mem' : mem,
               'cpu' : cpu,
               'series' : series,
               'regions' : regions,
               'regiongrp' : regiongrp,
               'region_name': region_name
             }
                )