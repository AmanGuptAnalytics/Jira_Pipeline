import dlt
import requests

def jira_source(api_token_key=dlt.secrets.value, api_email=dlt.secrets.value):
    return jira_search(api_token_key, api_email) 

def jira_search(api_token_key=dlt.secrets.value, api_email=dlt.secrets.value,jql_query='issuetype = Epic AND project = AMS'):
    url = f"https://dltjireapipeline.atlassian.net/rest/api/2/search?jql={jql_query}"
    auth = (api_email, api_token_key)
    headers = {
          "Accept": "application/json"
         }
    response = requests.get(url, auth=auth, headers=headers)
    response.raise_for_status()
    json_response = response.json()
    return response.json()
    

if __name__=='__main__':
    # configure the pipeline with your destination details
    #pipeline = dlt.pipeline(pipeline_name='jira', destination='bigquery', dataset_name='jira_data8')
    data = jira_search(api_token_key=dlt.secrets.value, api_email=dlt.secrets.value)
    for data1 in data:
        print(data1)
   # load_info = pipeline.run(jira_source())
    # pretty print the information on data that was loaded
   # print(load_info)
