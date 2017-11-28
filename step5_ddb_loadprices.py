from __future__ import print_function
import boto3
import ujson
import urllib

ec2_pricesURL = 'https://s3.amazonaws.com/awspricingspa/jsons/ec2-prices.json'
tmppricefile = '/tmp/tempprices.json'
urllib.urlretrieve(ec2_pricesURL, tmppricefile)
with open(tmppricefile) as json_file:
    pricefile = ujson.load(json_file)
myTable = 'ec2_pricing'
dynamodb = boto3.resource(service_name='dynamodb')
table = dynamodb.Table(myTable)
default_value = 'not applicable'


def ddb_loadprices(event, context):
    try:
        for item in pricefile:
            regiongrp = str(item['regiongrp'])
            name = str(item['name'])
            ondemand = str(item['ondemand'])
            od_day = str(item['od_day'])
            od_mth = str(item['od_mth'].strip("[,]"))
            od_yr = str(item['od_yr'].strip("[,]"))
            yrpartuphrly = str(item.setdefault('yrpartuphrly', default_value))
            yrpartupdaily = str(item.setdefault('yrpartupdaily', default_value))
            yrpartupmonthly = str(item.setdefault('yrpartupmonthly', default_value))
            yrpartupyearly = str(item.setdefault('yrpartupyearly', default_value))
            yrpartup_dp = str(item.setdefault('yrpartup_dp', default_value))

            triyrpartuphrly = str(item.setdefault('triyrpartuphrly', default_value))
            triyrpartupdaily = str(item.setdefault('triyrpartupdaily', default_value))
            triyrpartupmonthly = str(item.setdefault('triyrpartupmonthly', default_value))
            triyrpartupyearly = str(item.setdefault('triyrpartupyearly', default_value))
            triyrpartup3yearly = str(item.setdefault('triyrpartup3yearly', default_value))
            triyrpartup_dp = str(item.setdefault('triyrpartup_dp', default_value))

            yralluphrly = str(item.setdefault('yralluphrly', default_value))
            yrallupdaily = str(item.setdefault('yrallupdaily', default_value))
            yrallupmonthly = str(item.setdefault('yrallupmonthly', default_value))
            yrallupyearly = str(item.setdefault('yrallupyearly', default_value))

            triyralluphrly = str(item.setdefault('triyralluphrly', default_value))
            triyrallupdaily = str(item.setdefault('triyrallupdaily', default_value))
            triyrallupmonthly = str(item.setdefault('triyrallupmonthly', default_value))
            triyrallupyearly = str(item.setdefault('triyrallupyearly', default_value))
            triyrallup3yearly = str(item.setdefault('triyrallup3yearly', default_value))

            yrnouphrly = str(item.setdefault('yrnouphrly', default_value))
            yrnoupdaily = str(item.setdefault('yrnoupdaily', default_value))
            yrnoupmonthly = str(item.setdefault('yrnoupmonthly', default_value))
            yrnoupyearly = str(item.setdefault('yrnoupyearly', default_value))

            triyrnouphrly = str(item.setdefault('triyrnouphrly', default_value))
            triyrnoupdaily = str(item.setdefault('triyrnoupdaily', default_value))
            triyrnoupmonthly = str(item.setdefault('triyrnoupmonthly', default_value))
            triyrnoupyearly = str(item.setdefault('triyrnoupyearly', default_value))
            triyrnoup3yearly = str(item.setdefault('triyrnoup3yearly', default_value))

            table.update_item(Key={'regiongrp': regiongrp,
                                   'name': name
                                   },
                              UpdateExpression="SET \
                                            ondemand = :ondemand, \
                                            od_day = :od_day, \
                                            od_mth = :od_mth, \
                                            od_yr = :od_yr, \
                                            yrpartuphrly = :yrpartuphrly, \
                                            yrpartupdaily = :yrpartupdaily, \
                                            yrpartupmonthly = :yrpartupmonthly, \
                                            yrpartupyearly = :yrpartupyearly, \
                                            yrpartup_dp = :yrpartup_dp, \
                                            triyrpartuphrly = :triyrpartuphrly, \
                                            triyrpartupdaily = :triyrpartupdaily, \
                                            triyrpartupmonthly = :triyrpartupmonthly, \
                                            triyrpartupyearly = :triyrpartupyearly, \
                                            triyrpartup3yearly = :triyrpartup3yearly, \
                                            triyrpartup_dp = :triyrpartup_dp, \
                                            yralluphrly = :yralluphrly, \
                                            yrallupdaily = :yrallupdaily, \
                                            yrallupmonthly = :yrallupmonthly, \
                                            yrallupyearly = :yrallupyearly, \
                                            triyralluphrly = :triyralluphrly, \
                                            triyrallupdaily = :triyrallupdaily, \
                                            triyrallupmonthly = :triyrallupmonthly, \
                                            triyrallupyearly = :triyrallupyearly, \
                                            triyrallup3yearly = :triyrallup3yearly, \
                                            yrnouphrly = :yrnouphrly, \
                                            yrnoupdaily = :yrnoupdaily, \
                                            yrnoupmonthly = :yrnoupmonthly, \
                                            yrnoupyearly = :yrnoupyearly, \
                                            triyrnouphrly = :triyrnouphrly, \
                                            triyrnoupdaily = :triyrnoupdaily, \
                                            triyrnoupmonthly = :triyrnoupmonthly, \
                                            triyrnoupyearly = :triyrnoupyearly, \
                                            triyrnoup3yearly = :triyrnoup3yearly",
                              ExpressionAttributeValues={
                                  ':ondemand': ondemand,
                                  ':od_day': od_day,
                                  ':od_mth': od_mth,
                                  ':od_yr': od_yr,
                                  ':yrpartuphrly': yrpartuphrly,
                                  ':yrpartupdaily': yrpartupdaily,
                                  ':yrpartupmonthly': yrpartupmonthly,
                                  ':yrpartupyearly': yrpartupyearly,
                                  ':yrpartup_dp': yrpartup_dp,
                                  ':triyrpartuphrly': triyrpartuphrly,
                                  ':triyrpartupdaily': triyrpartupdaily,
                                  ':triyrpartupmonthly': triyrpartupmonthly,
                                  ':triyrpartupyearly': triyrpartupyearly,
                                  ':triyrpartup3yearly': triyrpartup3yearly,
                                  ':triyrpartup_dp': triyrpartup_dp,
                                  ':yralluphrly': yralluphrly,
                                  ':yrallupdaily': yrallupdaily,
                                  ':yrallupmonthly': yrallupmonthly,
                                  ':yrallupyearly': yrallupyearly,
                                  ':triyralluphrly': triyralluphrly,
                                  ':triyrallupdaily': triyrallupdaily,
                                  ':triyrallupmonthly': triyrallupmonthly,
                                  ':triyrallupyearly': triyrallupyearly,
                                  ':triyrallup3yearly': triyrallup3yearly,
                                  ':yrnouphrly': yrnouphrly,
                                  ':yrnoupdaily': yrnoupdaily,
                                  ':yrnoupmonthly': yrnoupmonthly,
                                  ':yrnoupyearly': yrnoupyearly,
                                  ':triyrnouphrly': triyrnouphrly,
                                  ':triyrnoupdaily': triyrnoupdaily,
                                  ':triyrnoupmonthly': triyrnoupmonthly,
                                  ':triyrnoupyearly': triyrnoupyearly,
                                  ':triyrnoup3yearly': triyrnoup3yearly,
                              })
    except KeyError, e:
        print('KeyError - reason "%s"' % str(e))
    except IndexError, e:
        print('IndexError - reason "%s"' % str(e))
    finally:
        print("Complete")