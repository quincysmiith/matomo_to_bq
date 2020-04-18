import json
from piwikapi.analytics import PiwikAnalytics
from dotenv import load_dotenv
import os
import requests
import pandas as pd
from google.oauth2 import service_account

# load variables into environment
load_dotenv()


def get_basic_metrics():
    """
    Collects basic metrics from Matomo installation on Marquinsmith.com
    and returns a pandas dataframe
    """

    token = os.getenv("TOKEN")

    # Build url string
    base_url = 'https://marquinsmith.com/piwik/piwik?module=API'
    site_num = '&idSite=1'
    return_format = '&format=json'
    period = '&period=day'
    date_range = '&date=last30'
    method = '&method=VisitsSummary.get'
    token_string = "&token_auth=" + token

    my_url = base_url + site_num + return_format + period + date_range + method + token_string


    # send request for report
    r = requests.get(my_url)

    # parse and tidy collected data
    data = pd.DataFrame(r.json()).T
    data = data.reset_index()

    data.columns = [
        "date",
        "uniq_visitors",
        "users",
        "visits",
        "actions",
        "visits_converted",
        "bounces",
        "sum_visit_length",
        "max_actions",
        "bounce_rate",
        "actions_per_visit",
        "avg_time_on_site",
    ]

    return data


def upload_to_bq(a_dataframe):
    """
    load dataframe into Big Query.
    Specifically the basicmetrics table.
    """

    service_account_json = os.getenv("SERVICE_ACCOUNT_JSON")
    project_id = os.getenv("PROJECT_ID")

    # create Google credentials from service account json file

    credentials = service_account.Credentials.from_service_account_file(
        service_account_json,
    )

    a_dataframe.to_gbq(
        "marquinsmith_dot_com.basicmetrics",
        project_id=project_id,
        if_exists="replace",
        credentials=credentials,
    )

    return None


if __name__ == "__main__":
    # set up healthchecks.io monitoring
    healthcheck_url = "https://hc-ping.com/42c0d9a3-3278-4b64-b61c-2ea94a324a17"
    requests.get(healthcheck_url + "/start")


    data = get_basic_metrics()
    upload_to_bq(data)

    # tell healthchecks.io the script is finished
    requests.get(healthcheck_url)
