#import libraries

import requests
import json
import datetime
import pandas as pd
import csv
from google.cloud import bigquery
import pytz
from google.oauth2 import service_account

#Define Parameter dict
params = dict()
params['access_token'] = 'zzzzzzxxxxxxxxx'
params['client_id'] = 'zzzzzzxxxxxxxxx'                  # not an actual client id
params['client_secret'] = 'zzzzzzxxxxxxxxx'     # not an actual client secret
params['graph_domain'] = 'https://graph.facebook.com'
params['graph_version'] = 'v17.0'
params['endpoint_base'] = params['graph_domain'] + '/' + params['graph_version'] + '/'
params['page_id'] = 'zzzzzzxxxxxxxxx'                  # not an actual page id
params['instagram_account_id'] = 'zzzzzzxxxxxxxxx'        # not an actual instagram business account id
params['ig_username'] = 'zzzzzzxxxxxxxxx'                 # not an actual instagram business account username

#Define Endpoint parameters
endpointParams = dict()
endpointParams['input_token'] = params['access_token']
endpointParams['access_token'] = params['access_token']

# Define URL
url = params['graph_domain'] + '/debug_token'

# Requests Data
data = requests.get(url, endpointParams)
access_token_data = json.loads(data.content)
access_token_data

# Define URL
url = params['endpoint_base'] + 'oauth/access_token'

# Define Endpoint Parameters
endpointParams = dict()
endpointParams['grant_type'] = 'fb_exchange_token'
endpointParams['client_id'] = params['client_id']
endpointParams['client_secret'] = params['client_secret']
endpointParams['fb_exchange_token'] = params['access_token']

# Requests Data
data = requests.get(url, endpointParams )
long_lived_token = json.loads(data.content)
long_lived_token

# Define URL
url = params['endpoint_base'] + params['instagram_account_id'] + '/media'

# Define Endpoint Parameters
endpointParams = dict()
endpointParams['fields'] = 'id,caption,media_type,media_url,permalink,thumbnail_url,media_product_type,timestamp,username,like_count,comments_count'
endpointParams['access_token'] = params['access_token']


# Requests Data
data = requests.get(url, endpointParams)
basic_insight = json.loads(data.content)
basic_insight

# creating a pandas dataframe to store the json output in a tabular format
df = pd.DataFrame(basic_insight['data'], columns=['id', 'caption', 'media_type', 'media_url','thumbnail_url','media_product_type','permalink', 'timestamp', 'username', 'like_count', 'comments_count'])
df.head()

post_insight = []

# Loop over media objects
for media in basic_insight['data']:
    media_id = media['id']
    media_type = media['media_type']
    url = params['endpoint_base'] + media_id + '/insights'

    endpointParams = dict()
    endpointParams['metric'] = 'engagement,impressions,reach,saved,shares'
    endpointParams['access_token'] = params['access_token']

    # Request data for posts
    data = requests.get(url, endpointParams)
    json_data_temp = json.loads(data.content)

    if 'data' in json_data_temp:
        insight_data = json_data_temp['data']
        for data_entry in insight_data:
            data_entry['id'] = media_id  # Add media ID to each insight data entry
        post_insight.extend(insight_data)

# Initialize Empty Container to convert post metadata into a dataframe
data = {}

# Loop over insights to fill container
for insight in post_insight:
    metric_name = insight['name']
    metric_value = insight['values'][0]['value']
    if insight['id'] in data:
        data[insight['id']][metric_name] = metric_value
    else:
        data[insight['id']] = {'id': insight['id'], metric_name: metric_value}

# Create DataFrame
df_post_insight = pd.DataFrame(list(data.values()))
df_post_insight = df_post_insight[['id','engagement', 'impressions', 'reach', 'saved', 'shares']]
df_post_insight = df_post_insight.fillna(0)  # Replace missing values with 0
df_post_insight

video_insight = []

# Loop over media objects
for media in basic_insight['data']:
    media_id = media['id']
    media_type = media['media_type']

    if media_type == 'VIDEO':  # Check if media type is reel
        url = params['endpoint_base'] + media_id + '/insights'

        endpointParams = dict()
        endpointParams['metric'] = 'plays,reach,saved,shares'
        endpointParams['access_token'] = params['access_token']

        # Request data for reels
        data = requests.get(url, endpointParams)
        json_data_temp = json.loads(data.content)

        if 'data' in json_data_temp:
            insight_data = json_data_temp['data']
            for data_entry in insight_data:
                data_entry['id'] = media_id  # Add media ID to each insight data entry
            video_insight.extend(insight_data)

# Initialize Empty Container to convert post metadata into a dataframe
data = {}

# Loop over insights to fill container
for insight in video_insight:
    metric_name = insight['name']
    metric_value = insight['values'][0]['value']
    if insight['id'] in data:
        data[insight['id']][metric_name] = metric_value
    else:
        data[insight['id']] = {'id': insight['id'], metric_name: metric_value}

# Create DataFrame
df_video_insight = pd.DataFrame(list(data.values()))
df_video_insight = df_video_insight[['id','plays', 'reach', 'saved', 'shares']]
df_video_insight = df_video_insight.fillna(0)  # Replace missing values with 0
df_video_insight

# Merge df_post_insight and df_video_insight based on the 'id' column
combined_df = pd.merge(df_post_insight[['id', 'engagement', 'impressions', 'reach', 'saved', 'shares']],
                       df_video_insight[['id', 'reach', 'saved', 'shares', 'plays']],
                       on='id',
                       how='outer')

# Combine the corresponding values from 'reach', 'saved', and 'shares' columns
combined_df['reach'] = combined_df['reach_x'].fillna(combined_df['reach_y'])
combined_df['saved'] = combined_df['saved_x'].fillna(combined_df['saved_y'])
combined_df['shares'] = combined_df['shares_x'].fillna(combined_df['shares_y'])

# Drop the unnecessary columns
combined_df = combined_df.drop(['reach_x', 'reach_y', 'saved_x', 'saved_y', 'shares_x', 'shares_y'], axis=1)

#merge the combined dataframe with the media detial table that is df

df_complete = pd.merge(df,combined_df, on='id',how='outer')
df_complete


#push the final dataframe to google biquery 
key_path= "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz   "  #service account key file loaction
project_id="zzzzzzzzzzzzzzzzzzzzzzzzzz"                            #bigquery project id
dataset_id = "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"               #bigquery dataset id
table_id="zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"      #bigquery table id
print("********* NAME OF TABLE IS", table_id)

data = df_complete

def load_table_dataframe_config(key_path,project_id,table_id, data):
    credentials = service_account.Credentials.from_service_account_file(
        key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )

    # Construct a BigQuery client object.
    client = bigquery.Client(credentials=credentials, project=project_id)

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE")

    job = client.load_table_from_dataframe(
        data, table_id, job_config=job_config
    )
    job.result()

    data = client.get_table(table_id)
    return data

data = load_table_dataframe_config(key_path,project_id, table_id,data)

# STORY DATA
# Define URL
url = params['endpoint_base'] + params['instagram_account_id'] + '/stories'

# Define Endpoint Parameters
endpointParams = dict()
endpointParams['fields'] = 'id,media_type,media_url,permalink'
endpointParams['access_token'] = params['access_token']

# Requests Data
response = requests.get(url, endpointParams)
story_insight = json.loads(response.content)

# Fetch insights for each story
media_insight = []
data = {}  # Change the variable name to avoid conflicts

for story in story_insight['data']:
    media_id = story['id']
    media_type = story['media_type']
    media_url = story['media_url']
    permalink = story['permalink']

    url = params['endpoint_base'] + media_id + '/insights'

    endpointParams = dict()
    endpointParams['metric'] = 'impressions,reach,replies'
    endpointParams['access_token'] = params['access_token']

    # Request insights data
    response = requests.get(url, endpointParams)
    insights = json.loads(response.content)

    if 'data' in insights:
        insight_data = insights['data']
        for insight in insight_data:  # Iterate over insight_data instead of media_insight
            metric_name = insight['name']
            metric_value = insight['values'][0]['value']
            if media_id in data:  # Use media_id instead of insight['id']
                data[media_id][metric_name] = metric_value
            else:
                data[media_id] = {'id': media_id, 'media_url': media_url, 'permalink': permalink, metric_name: metric_value}

# Create dataframe
df_story_insight = pd.DataFrame(list(data.values()))
df_story_insight = df_story_insight[['id', 'media_url', 'permalink', 'impressions', 'reach', 'replies']]
df_story_insight = df_story_insight.fillna(0)  # Replace missing values with 0
df_story_insight

#push the story insight data into bigquery
key_path= "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"
project_id="zzzzzzzzzzzzzzzzzzzzzzzzzz"
dataset_id = "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"
table_id="zzzzzzzzzzzzzzzzzzzzzzzzzzzz"
print("********* NAME OF TABLE IS", table_id)

data = df_story_insight

def load_table_dataframe_config(key_path,project_id,table_id, data):
    credentials = service_account.Credentials.from_service_account_file(
        key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )

    # Construct a BigQuery client object.
    client = bigquery.Client(credentials=credentials, project=project_id)

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE")

    job = client.load_table_from_dataframe(
        data, table_id, job_config=job_config
    )
    job.result()

    data = client.get_table(table_id)
    return data

data = load_table_dataframe_config(key_path,project_id, table_id,data)

df_story_insight
