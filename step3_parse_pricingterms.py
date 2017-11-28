import urllib
from collections import defaultdict
from multiprocessing import Process

import boto3
import ujson

s3cl = boto3.client('s3')
bucket = 'awspricingspa'
parsedproductsURL = 'https://s3.amazonaws.com/awspricingspa/jsons/ec2-products.json'
parsedtermsURL = 'https://s3.amazonaws.com/awspricingspa/jsons/parsed-terms.json'
tmpparsedproductsfile = '/tmp/temp-prdts.json'
tmpparsedtermsfile = '/tmp/tempterms.json'
final_odpricefile = 'jsons/ec2-odprices.json'
final_rsvdpricefile = 'jsons/ec2-resprices.json'
final_pricefile = 'jsons/ec2-prices.json'

# start = time.time()
urllib.urlretrieve(parsedproductsURL, tmpparsedproductsfile)
urllib.urlretrieve(parsedtermsURL, tmpparsedtermsfile)
with open(tmpparsedtermsfile) as json_file1:
    termsf = ujson.load(json_file1)
with open(tmpparsedproductsfile) as json_file2:
    prodf = ujson.load(json_file2)


def reserved():
    dicttriyrnoup = []
    dictyrnoup = []
    dictyrpartup = []
    dictyrpartup_dp = []
    dicttriyrpartup = []
    dicttriyrpartup_dp = []
    dictyrallup = []
    dicttriyrallup = []
    tempdictsinglepart = []
    tempdicttripart = []
    try:
        for y in prodf:
            for x, info in termsf['Reserved'].items():
                if y['name'] == x:
                    for t in info.values():
                        for z, info in t['priceDimensions'].items():
                            for yrnoup in info['pricePerUnit'].values():
                                if "4NA7Y494T4.6YS6EN2CT7" in z:  # 1 year no upfront down payment
                                    sep = '.'
                                    yrnoupname = z.split(sep, 1)[0]
                                    yrnoupregiongrp = y['regiongrp']
                                    yrnouphrly = (float(yrnoup))
                                    yrnoupdaily = int("24") * (float(yrnoup))
                                    yrnoupmonthly = float("728.93") * (float(yrnoup))
                                    yrnoupyearly = int("12") * (float(yrnoupmonthly))
                                    dictyrnoup.append({
                                        'name': yrnoupname,
                                        'regiongrp': yrnoupregiongrp,
                                        'yrnouphrly': '{0:,.2f}'.format(yrnouphrly),
                                        'yrnoupdaily': '{0:,.2f}'.format(yrnoupdaily),
                                        'yrnoupmonthly': '{0:,.2f}'.format(yrnoupmonthly),
                                        'yrnoupyearly': '{0:,.2f}'.format(yrnoupyearly)
                                    })
                            for triyrnoup in info['pricePerUnit'].values():
                                if "BPH4J8HBKS.6YS6EN2CT7" in z:  # 3 year no upfront down payment
                                    sep = '.'
                                    triyrnoupname = z.split(sep, 1)[0]
                                    triyrnoupregiongrp = y['regiongrp']
                                    triyrnouphrly = (float(triyrnoup))
                                    triyrnoupdaily = int("24") * (float(triyrnouphrly))
                                    triyrnoupmonthly = float("728.93") * (float(triyrnouphrly))
                                    triyrnoupyearly = int("12") * (float(triyrnoupmonthly))
                                    triyrnoup3yearly = int("36") * (float(triyrnoupmonthly))
                                    dicttriyrnoup.append({
                                        'name': triyrnoupname,
                                        'regiongrp': triyrnoupregiongrp,
                                        'triyrnouphrly': '{0:,.2f}'.format(triyrnouphrly),
                                        'triyrnoupdaily': '{0:,.2f}'.format(triyrnoupdaily),
                                        'triyrnoupmonthly': '{0:,.2f}'.format(triyrnoupmonthly),
                                        'triyrnoupyearly': '{0:,.2f}'.format(triyrnoupyearly),
                                        'triyrnoup3yearly': '{0:,.2f}'.format(triyrnoup3yearly)
                                    })
                            for yrpartup_dp in info['pricePerUnit'].values():
                                if "HU7G6KETJZ.2TG2D8R56U" in z:  # 1 year part upfront down payment
                                    sep = '.'
                                    yrpartup_dpname = z.split(sep, 1)[0]
                                    yrpartup_dpregiongrp = y['regiongrp']
                                    yrpartuphrly_dp = (float(yrpartup_dp)) / int("8784")
                                    yrpartupdaily_dp = (float(yrpartup_dp)) / int("365")
                                    yrpartupmonthly_dp = (float(yrpartup_dp)) / int("12")
                                    yrpartup_dp = float(yrpartup_dp)
                                    dictyrpartup_dp.append({
                                        'name': yrpartup_dpname,
                                        'regiongrp': yrpartup_dpregiongrp,
                                        'yrpartuphrly_dp': (float(yrpartuphrly_dp)),
                                        'yrpartupdaily_dp': (float(yrpartupdaily_dp)),
                                        'yrpartupmonthly_dp': (float(yrpartupmonthly_dp)),
                                        'yrpartup_dp': (float(yrpartup_dp)),
                                    })
                            for yrpartup in info['pricePerUnit'].values():
                                if "HU7G6KETJZ.6YS6EN2CT7" in z:  # 1 year part upfront
                                    sep = '.'
                                    yrpartupname = z.split(sep, 1)[0]
                                    yrpartupregiongrp = y['regiongrp']
                                    yrpartuphrly = (float(yrpartup))
                                    yrpartupdaily = int("24") * (float(yrpartup))
                                    yrpartupmonthly = float("730") * (float(yrpartup))
                                    yrpartupyearly = int("12") * (float(yrpartupmonthly))
                                    dictyrpartup.append({
                                        'name': yrpartupname,
                                        'regiongrp': yrpartupregiongrp,
                                        'yrpartuphrly': (float(yrpartuphrly)),
                                        'yrpartupdaily': (float(yrpartupdaily)),
                                        'yrpartupmonthly': (float(yrpartupmonthly)),
                                        'yrpartupyearly': (float(yrpartupyearly))
                                    })
                            for triyrpartup_dp in info['pricePerUnit'].values():
                                if "38NPMPTW36.2TG2D8R56U" in z:  # 3 year part upfront down payment
                                    sep = '.'
                                    triyrpartup_dpname = z.split(sep, 1)[0]
                                    triyrpartup_dpregiongrp = y['regiongrp']
                                    triyrpartup_dp = float(triyrpartup_dp)
                                    triyrpartupyearly_dp = (float(triyrpartup_dp)) / int("3")
                                    triyrpartupmonthly_dp = (float(triyrpartup_dp)) / int("36")
                                    triyrpartupdaily_dp = (float(triyrpartup_dp)) / int("1095")
                                    triyrpartuphrly_dp = (float(triyrpartup_dp)) / int("26352")
                                    dicttriyrpartup_dp.append({
                                        'name': triyrpartup_dpname,
                                        'regiongrp': triyrpartup_dpregiongrp,
                                        'triyrpartuphrly_dp': (float(triyrpartuphrly_dp)),
                                        'triyrpartupdaily_dp': (float(triyrpartupdaily_dp)),
                                        'triyrpartupmonthly_dp': (float(triyrpartupmonthly_dp)),
                                        'triyrpartupyearly_dp': (float(triyrpartupyearly_dp)),
                                        'triyrpartup_dp': (float(triyrpartup_dp)),
                                    })
                            for triyrpartup in info['pricePerUnit'].values():
                                if "38NPMPTW36.6YS6EN2CT7" in z:  # 3 year part upfront
                                    sep = '.'
                                    triyrpartupname = z.split(sep, 1)[0]
                                    triyrpartupregiongrp = y['regiongrp']
                                    triyrpartuphrly = (float(triyrpartup))
                                    triyrpartupdaily = int("24") * (float(triyrpartup))
                                    triyrpartupmonthly = float("728.93") * (float(triyrpartup))
                                    triyrpartupyearly = int("12") * (float(triyrpartupmonthly))
                                    triyrpartup3yearly = int("3") * (float(triyrpartupyearly))
                                    dicttriyrpartup.append({
                                        'name': triyrpartupname,
                                        'regiongrp': triyrpartupregiongrp,
                                        'triyrpartuphrly': (float(triyrpartuphrly)),
                                        'triyrpartupdaily': (float(triyrpartupdaily)),
                                        'triyrpartupmonthly': (float(triyrpartupmonthly)),
                                        'triyrpartupyearly': (float(triyrpartupyearly)),
                                        'triyrpartup3yearly': (float(triyrpartup3yearly))
                                    })
                            for yrallup in info['pricePerUnit'].values():
                                if "6QCMYABX3D.2TG2D8R56U" in z:  # 1 year all upfront down payment
                                    sep = '.'
                                    yrallupname = z.split(sep, 1)[0]
                                    yrallupregiongrp = y['regiongrp']
                                    yralluphrly = (float(yrallup)) / int("8784")
                                    yrallupdaily = (float(yrallup)) / int("365")
                                    yrallupmonthly = (float(yrallup)) / int("12")
                                    yrallupyearly = (float(yrallup))
                                    dictyrallup.append({
                                        'name': yrallupname,
                                        'regiongrp': yrallupregiongrp,
                                        'yralluphrly': '{0:,.2f}'.format(yralluphrly),
                                        'yrallupdaily': '{0:,.2f}'.format(yrallupdaily),
                                        'yrallupmonthly': '{0:,.2f}'.format(yrallupmonthly),
                                        'yrallupyearly': '{0:,.2f}'.format(yrallupyearly)
                                    })
                            for triyrallup in info['pricePerUnit'].values():
                                if "NQ3QZPMQV9.2TG2D8R56U" in z:  # 1 year part upfront down payment
                                    sep = '.'
                                    triyrallupname = z.split(sep, 1)[0]
                                    triyrallupregiongrp = y['regiongrp']
                                    triyralluphrly = (float(triyrallup)) / int("26352")
                                    triyrallupdaily = (float(triyrallup)) / int("1095")
                                    triyrallupmonthly = (float(triyrallup)) / int("36")
                                    triyrallupyearly = (float(triyrallup)) / int("3")
                                    triyrallup3yearly = (float(triyrallup))
                                    dicttriyrallup.append({
                                        'name': triyrallupname,
                                        'regiongrp': triyrallupregiongrp,
                                        'triyralluphrly': '{0:,.2f}'.format(triyralluphrly),
                                        'triyrallupdaily': '{0:,.2f}'.format(triyrallupdaily),
                                        'triyrallupmonthly': '{0:,.2f}'.format(triyrallupmonthly),
                                        'triyrallupyearly': '{0:,.2f}'.format(triyrallupyearly),
                                        'triyrallup3yearly': '{0:,.2f}'.format(triyrallup3yearly),
                                    })

    except AttributeError:
        pass
        print ("Finished first pass")
    try:
        for partup in dictyrpartup:
            for dp in dictyrpartup_dp:
                if partup['name'] == dp['name']:
                    regiongrp1 = dp['regiongrp']
                    name1 = partup['name']
                    # Hourly
                    yph = (partup['yrpartuphrly'])
                    yph_dp = (dp['yrpartuphrly_dp'])
                    yrpartuphrly = (yph + yph_dp)
                    # Daily
                    ypd = (partup['yrpartupdaily'])
                    ypd_dp = (dp['yrpartupdaily_dp'])
                    yrpartupdaily = (ypd + ypd_dp)
                    # Monthly
                    ypm = (partup['yrpartupmonthly'])
                    ypm_dp = (dp['yrpartupmonthly_dp'])
                    yrpartupmonthly = (ypm + ypm_dp)
                    # Yearly
                    ypy = (partup['yrpartupyearly'])
                    ypy_dp = (dp['yrpartup_dp'])
                    yrpartupyearly = (ypy + ypy_dp)
                    yrpartup_dp = dp['yrpartup_dp']
                    tempdictsinglepart.append({
                        'regiongrp': regiongrp1,
                        'name': name1,
                        'yrpartuphrly': '{0:,.2f}'.format(yrpartuphrly),
                        'yrpartupdaily': '{0:,.2f}'.format(yrpartupdaily),
                        'yrpartupmonthly': '{0:,.2f}'.format(yrpartupmonthly),
                        'yrpartupyearly': '{0:,.2f}'.format(yrpartupyearly),
                        'yrpartup_dp': '{0:,.2f}'.format(yrpartup_dp),
                    })

        for tripartup in dicttriyrpartup:
            for tridp in dicttriyrpartup_dp:
                if tripartup['name'] == tridp['name']:
                    regiongrp2 = tripartup['regiongrp']
                    name2 = tripartup['name']
                    # Hourly
                    triyph = (tripartup['triyrpartuphrly'])
                    triyph_dp = (tridp['triyrpartuphrly_dp'])
                    triyrpartuphrly = (triyph + triyph_dp)
                    # Daily
                    triypd = (tripartup['triyrpartupdaily'])
                    triypd_dp = (tridp['triyrpartupdaily_dp'])
                    triyrpartupdaily = (triypd + triypd_dp)
                    # Monthly
                    triypm = (tripartup['triyrpartupmonthly'])
                    triypm_dp = (tridp['triyrpartupmonthly_dp'])
                    triyrpartupmonthly = (triypm + triypm_dp)
                    # Yearly
                    triypy = (tripartup['triyrpartup3yearly'])
                    triypy_dp = (tridp['triyrpartup_dp'])
                    triyrpartupyearly = ((triypy + triypy_dp) / 3)
                    triyrpartup3yearly = (triypy + triypy_dp)
                    triyrpartup_dp = tridp['triyrpartup_dp']
                    tempdicttripart.append({
                        'regiongrp': regiongrp2,
                        'name': name2,
                        'triyrpartuphrly': '{0:,.2f}'.format(triyrpartuphrly),
                        'triyrpartupdaily': '{0:,.2f}'.format(triyrpartupdaily),
                        'triyrpartupmonthly': '{0:,.2f}'.format(triyrpartupmonthly),
                        'triyrpartupyearly': '{0:,.2f}'.format(triyrpartupyearly),
                        'triyrpartup3yearly': '{0:,.2f}'.format(triyrpartup3yearly),
                        'triyrpartup_dp': '{0:,.2f}'.format(triyrpartup_dp),
                    })
    finally:
        d = defaultdict(dict)
        for l in (dictyrnoup, dicttriyrnoup, tempdictsinglepart, tempdicttripart, dictyrallup, dicttriyrallup):
            for elem in l:
                d[elem['name']].update(elem)
                rsvddict = d.values()
                with open('/tmp/res-prices.json', "w") as f:
                    ujson.dump(rsvddict, f)
                    # rsvdresponse = open('/tmp/res-prices.json')
                    # rsvd = rsvdresponse.read()
                    # s3cl.put_object(Bucket=bucket, Key=final_rsvdpricefile, Body=rsvd)


def ondemand():
    oddicts = []
    try:
        for y in prodf:
            for x, info in termsf['OnDemand'].items():
                if y['name'] == x:
                    for z in info.values():
                        for b in z['priceDimensions'].values():
                            for odobject in b['pricePerUnit'].values():
                                regiongrp = y['regiongrp']
                                oddaily = int("24") * (float(odobject))
                                odmonthly = int("730") * (float(odobject))
                                odyearly = int("8760") * (float(odobject))
                                oddicts.append({
                                    'regiongrp': regiongrp,
                                    'name': x,
                                    'ondemand': str(float(odobject)),
                                    'od_day': '{0:,.2f}'.format(oddaily),
                                    'od_mth': '{0:,.2f}'.format(odmonthly),
                                    'od_yr': '{0:,.2f}'.format(odyearly)
                                })
    finally:
        with open('/tmp/odprices.json', "w") as f:
            ujson.dump(oddicts, f)
            # odresponse = open('/tmp/odprices.json')
            # od = odresponse.read()
            # s3cl.put_object(Bucket=bucket, Key=final_odpricefile, Body=od)


def mergeprices():
    with open('/tmp/res-prices.json') as json_file3:
        rsvddict = ujson.load(json_file3)
    with open('/tmp/odprices.json') as json_file4:
        oddicts = ujson.load(json_file4)
    d = defaultdict(dict)
    for l in (oddicts, rsvddict):
        for elem in l:
            d[elem['name']].update(elem)
            finalprices = d.values()
            with open('/tmp/finalprices.json', "w") as f:
                ujson.dump(finalprices, f)
    finalpriceresponse = open('/tmp/finalprices.json')
    finalprice = finalpriceresponse.read()
    s3cl.put_object(Bucket=bucket, Key=final_pricefile, Body=finalprice)


# def parse_ec2_terms():
def parse_ec2pricing_terms(event, context):
    try:
        p1 = Process(target=reserved)
        p1.start()
        p2 = Process(target=ondemand)
        p2.start()
        p1.join()
        p2.join()
    finally:
        mergeprices()
        # parse_ec2_terms()

        # end = time.time()
        # print(end - start)
