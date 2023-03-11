import dlt
import requests
import json

@dlt.source
def jira_source(api_token_key=dlt.secrets.value, api_email=dlt.secrets.value):
    start_at = -50
    max_results = 100
    page_size = 50
    while start_at<=max_results:
        start_at = start_at + page_size
        return jira_search_query(api_token_key=dlt.secrets.value, api_email=dlt.secrets.value, start_at=start_at)
        


@dlt.resource(write_disposition="append")
def jira_search_query(api_token_key=dlt.secrets.value, api_email=dlt.secrets.value,start_at=0, max_results=100):
    url = f"https://dlijireapipeline.atlassian.net/rest/api/2/search"
    
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
    

if __name__=='__main__':
    # configure the pipeline with your destination details
    pipeline = dlt.pipeline(pipeline_name='jira', destination='bigquery', dataset_name='jira_data32',full_refresh=True)

    load_info = pipeline.run(jira_source())
    # pretty print the information on data that was loaded
    print(load_info)
