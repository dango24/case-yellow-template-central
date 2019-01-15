from botocore.vendored import requests

def handler(event, context):
    print("Received getNextSpeedTestWebSite GET request, from user: {}".format(event.get("user")))

    headers = {"Content-Type": "application/json",
               "Case-Yellow-User": event.get("user")}

    response = requests.post("http://internal-cy-internal-load-balancer-1608404301.eu-central-1.elb.amazonaws.com:9080/central/next-web-site", data={}, headers=headers)

    return response.json

