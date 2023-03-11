import dlt
import requests
import json


@dlt.source
def jira_source(api_token_key=dlt.secrets.value, api_email=dlt.secrets.value):
    return jira_search(api_token_key, api_email, page_size=50)


@dlt.resource(write_disposition="replace")
def _jira_search_query(api_token_key=dlt.secrets.value, api_email=dlt.secrets.value,start_at=0, max_results=100):
    url = f"https://dlijireapipeline.atlassian.net/rest/api/2/search"
    
    api_token_key="ATATT3xFfGF0uwlb8zhOyzLtr49AolLQ4aUvaT9oNYDFifCjJ8Hk8xNGG_aVdC-O23HWafR4nePtcX8wOvMnlPjRwVve94xba0CyVZ2RYRYYiUrG82wbPvN1undxQyw834_8nStUZQ9ZVXDgWsltCqxlRKK02Px3rQ-s1Tt-3nKOEXeLO8w1BBM=24656807"

    api_email='amanguptanalytics@gmail.com'
    auth = (api_email, api_token_key)
    headers = {
          "Accept": "application/json"
         }
    query_params = {
        "jql": "issuetype = Epic AND project = DLTJIR",
        "startAt": start_at,
        "maxResults": max_results,
    }

    response = requests.get(url, params=query_params, auth=auth, headers=headers)
    response.raise_for_status()
    
    yield response.json()
    
@dlt.source
def jira_search(api_token_key=dlt.secrets.value, api_email=dlt.secrets.value, page_size=50):
    start_at = 0
    while True:
        issues = _jira_search_query(api_token_key=dlt.secrets.value, api_email=dlt.secrets.value, start_at=start_at)
        if not issues:
            break
        else:
            start_at += page_size

if __name__=='__main__':
    # configure the pipeline with your destination details
    pipeline = dlt.pipeline(pipeline_name='jira', destination='bigquery', dataset_name='jira_data67')

    load_info = pipeline.run(jira_source())
    # pretty print the information on data that was loaded
    print(load_info)
