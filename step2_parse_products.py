import boto3, ujson, jmespath, urllib

bucket='awspricingspa'
parsedproductsfile = 'jsons/ec2-products.json'
productsfileURL = 'https://s3.amazonaws.com/awspricingspa/jsons/parsed-products.json'
s3 = boto3.resource('s3')
s3cl = boto3.client('s3')
urllib.urlretrieve(productsfileURL, '/tmp/temparse.json')
with open('/tmp/temparse.json') as json_file:
    data = ujson.load(json_file)

def parse_ec2products(event,context):
    mydicts = []
    try:
        for name, info in data.items():
                attrib = jmespath.search("attributes", info)
                location = str(jmespath.search("location", attrib))
                type = str(jmespath.search("instanceType", attrib))
                if type.startswith('m'):
                    series = 'M Series'
                elif type.startswith('c'):
                    series = 'C Series'
                elif type.startswith('t'):
                    series = 'T Series'
                elif type.startswith('r'):
                    series = 'R Series'
                elif type.startswith('d'):
                    series = 'D Series'
                elif type.startswith('g'):
                    series = 'G Series'
                elif type.startswith('h'):
                    series = 'H Series'
                elif type.startswith('i'):
                    series = 'I Series'
                elif type.startswith('p'):
                    series = 'P Series'
                elif type.startswith('x'):
                    series = 'X Series'
                elif type.startswith('f'):
                    series = 'F Series'
                else:
                    series = 'UNKNOWN'

                if (str(jmespath.search("location", attrib)) == 'US East (N. Virginia)'):
                    regions = "US-EAST-1"
                    regiongrp = "USCA"
                    region_name = "US East (N. Virginia)"
                elif (str(jmespath.search("location", attrib)) == 'US East (Ohio)'):
                    regions = "US-EAST-2"
                    regiongrp = "USCA"
                    region_name = "US East (Ohio)"
                elif (str(jmespath.search("location", attrib)) == 'US West (N. California)'):
                    regions = "US-WEST-1"
                    regiongrp = "USCA"
                    region_name = "US West (N. California)"
                elif (str(jmespath.search("location", attrib)) == 'US West (Oregon)'):
                    regions = "US-WEST-2"
                    regiongrp = "USCA"
                    region_name = "US West (Oregon)"
                elif (str(jmespath.search("location", attrib)) == 'Canada (Central)'):
                    regions = "CA-CENTRAL-1"
                    regiongrp = "USCA"
                    region_name = "Canada (Central)"
                elif (str(jmespath.search("location", attrib)) == 'Asia Pacific (Mumbai)'):
                    regions = "AP-SOUTH-1"
                    regiongrp = "NONUSCA"
                    region_name = "Asia Pacific (Mumbai)"
                elif (str(jmespath.search("location", attrib)) == 'Asia Pacific (Seoul)'):
                    regions = "AP-NORTHEAST-2"
                    regiongrp = "NONUSCA"
                    region_name = "Asia Pacific (Seoul)"
                elif (str(jmespath.search("location", attrib)) == 'Asia Pacific (Singapore)'):
                    regions = "AP-SOUTHEAST-1"
                    regiongrp = "NONUSCA"
                    region_name = "Asia Pacific (Singapore)"
                elif (str(jmespath.search("location", attrib)) == 'Asia Pacific (Sydney)'):
                    regions = "AP-SOUTHEAST-2"
                    regiongrp = "NONUSCA"
                    region_name = "Asia Pacific (Sydney)"
                elif (str(jmespath.search("location", attrib)) == 'Asia Pacific (Tokyo)'):
                    regions = "AP-NORTHEAST-1"
                    regiongrp = "NONUSCA"
                    region_name = "Asia Pacific (Tokyo)"
                elif (str(jmespath.search("location", attrib)) == 'EU (Frankfurt)'):
                    regions = "EU-CENTRAL-1"
                    regiongrp = "NONUSCA"
                    region_name = "EU (Frankfurt)"
                elif (str(jmespath.search("location", attrib)) == 'EU (Ireland)'):
                    regions = "EU-WEST-1"
                    regiongrp = "NONUSCA"
                    region_name = "EU (Ireland)"
                elif (str(jmespath.search("location", attrib)) == 'EU (London)'):
                    regions = "EU-WEST-2"
                    regiongrp = "NONUSCA"
                    region_name = "EU (London)"
                    # elif (str(jmespath.search("location", attrib)) == 'South America'):
                else:
                    regions = "SA-EAST-1"
                    regiongrp = "NONUSCA"
                    region_name = "South America"


                if (str(jmespath.search("operatingSystem", attrib)) == 'Linux') \
                        and (str(jmespath.search("preInstalledSw", attrib)) == 'NA') \
                        and (str(jmespath.search("tenancy", attrib)) == 'Shared') \
                        and (str(jmespath.search("clockSpeed", attrib)) != 'None' ) \
                        and (str(jmespath.search("licenseModel", attrib)) == 'No License required'):
                    mydicts.append({
                        'OS': jmespath.search("operatingSystem", attrib),
                        'type': jmespath.search("instanceType", attrib),
                        'cpu': jmespath.search("vcpu", attrib),
                        'mem': str(jmespath.search("memory", attrib))[:-4],
                        'clockSpeed': str(jmespath.search("clockSpeed", attrib)).strip("['']"),
                        'storage': str(jmespath.search("storage", attrib)).strip("['']"),
                        'Network': jmespath.search("networkPerformance", attrib),
                        'ebsOptimized': str(jmespath.search("dedicatedEbsThroughput", attrib)),
                        'ens': str(jmespath.search("enhancedNetworkingSupported", attrib)).strip("['']"),
                        'family': jmespath.search("instanceFamily", attrib),
                        'name': name,
                        'series': series,
                        'regions': regions,
                        'regiongrp': regiongrp,
                        'region_name': region_name
                    })
                elif (str(jmespath.search("operatingSystem", attrib)) == 'Windows') \
                        and (str(jmespath.search("preInstalledSw", attrib)) == 'NA') \
                        and (str(jmespath.search("tenancy", attrib)) == 'Shared') \
                        and (str(jmespath.search("licenseModel", attrib)) == 'No License required'):
                    mydicts.append({
                        'OS': jmespath.search("operatingSystem", attrib),
                        'type': jmespath.search("instanceType", attrib),
                        'cpu': jmespath.search("vcpu", attrib),
                        'mem': jmespath.search("memory", attrib)[:-4],
                        'clockSpeed': str(jmespath.search("clockSpeed", attrib)).strip("['']"),
                        'storage': str(jmespath.search("storage", attrib)).strip("['']"),
                        'Network': jmespath.search("networkPerformance", attrib),
                        'ebsOptimized': str(jmespath.search("dedicatedEbsThroughput", attrib)),
                        'ens': str(jmespath.search("enhancedNetworkingSupported", attrib)).strip("['']"),
                        'family': jmespath.search("instanceFamily", attrib),
                        'name': name,
                        'series': series,
                        'regions': regions,
                        'regiongrp': regiongrp,
                        'region_name': region_name
                    })
    finally:
        with open('/tmp/parsed-ec2products.json', "w") as f:
            ujson.dump(mydicts, f)
        response = open('/tmp/parsed-ec2products.json')
        ff = response.read()
        s3cl.put_object(Bucket=bucket, Key=parsedproductsfile, Body=ff)
#parse_ec2products()
