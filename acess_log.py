import requests
import json 
import os
import sys
from calendar import timegm
from datetime import datetime, timedelta, timezone
import pytz
from tzlocal import get_localzone
plugin_folder_name = 'github_ddr_1730963632_general'
plugin_id= "github_ddr_1730963632"

dirname, filename = os.path.split(os.path.abspath(__file__))
sys.path.append(dirname)

from github_plugin import Plugin
commonFunction_obj = Plugin(plugin_folder_name)

statsPath = str(dirname + '/stats.json')

try:
    from includes.share.utils import LSHashing  
    LSHashing_enabled = 1
except:
    LSHashing_enabled = 0

def get_settings_data():
    settings_file = str(dirname + "/settings.json")
    settings_data = read_json(settings_file)
    return settings_data

def read_json(path):
    data = {}
    if os.path.isfile(path):
        with open(path) as f:
            data = json.load(f)
    return data

def get_config_data():
    settings_file = str(dirname + "/config.json")
    settings_data = read_json(settings_file)
    return settings_data 

pluginName = get_config_data()["id"]

def write_json(path, data):
    if os.path.isfile(path):
        with open(path, "w") as json_file:
            json.dump(data, json_file)

# def get_github_organization_repositories(headers, username):
#     if headers and username:
#         url_org = f'https://api.github.com/users/{username}/repos'
#         response = requests.get(url_org, headers=headers)
#         if response.status_code == 200:
#             repositories = response.json()
#             repository_names = []
#             repository_paths = []
#             for repo in repositories:
#                 repository_name = repo.get('name', '')
#                 repository_path = repo.get('url', '')
                
#                 repository_names.append(repository_name)
#                 repository_paths.append(repository_path)
#             if repository_names:
#                 res = {
#                     "status": 1,
#                     "repositories": repository_names ,
#                     "path" : repository_paths   
#                 }
#             else:
#                 res = {
#                     "status": 0,
#                     "repositories": "No repositories found",
#                 }
#         else:
#             res = {
#                 "status": 0,
#                 "repositories": "Username or token is invalid input",
#             }
#         return res
#     else:
#         return {
#             "status": 0,
#             "repositories": "Invalid username or token",
#         }   

def get_github_repositories(headers, username):
    """Helper function to fetch organization repositories with pagination and error handling."""
    
    if not headers or not username:
        return {
            "status": 0,
            "repositories": "Invalid username or headers."
        }
    url_org = f'https://api.github.com/users/{username}/repos'
    
    repository_names = []
    repository_paths = []
    page = 1
    while True:
        try:
            response = requests.get(f"{url_org}?page={page}&per_page=100", headers=headers)
            
            
            if response.status_code == 200:
                repositories = response.json()
                
                if not repositories:
                    break
                for repo in repositories:
                    repository_name = repo.get('name', '')
                    repository_path = repo.get('url', '')
                    
                    repository_names.append(repository_name)
                    repository_paths.append(repository_path)

                if 'Link' in response.headers:
                    links = response.headers['Link']
                    if 'rel="next"' not in links:
                        break 
                else:
                    break 
            else:
                return {
                    "status": 0,
                    "repositories": f"Error fetching repositories: {response.status_code} - {response.text}"
                }
        except requests.exceptions.HTTPError as errh:
            if repository_names:
                return {
                    "status": 1,
                    "repositories": repository_names,
                    "path" : repository_paths
                }
            else:
                return {
                    "status": 0,
                    "message": str(errh)
                }
        except requests.exceptions.HTTPError as errh:
            if repository_names:
                return {
                    "status": 1,
                    "repositories": repository_names,
                    "path" : repository_paths
                }
            else:
                return {
                    "status": 0,
                    "message": str(errh)
                }
        except requests.exceptions.HTTPError as errh:
            if repository_names:
                return {
                    "status": 1,
                    "repositories": repository_names,
                    "path" : repository_paths
                }
            else:
                return {
                    "status": 0,
                    "message": str(errh)
                }
        except requests.exceptions.ReadTimeout as errrt:
            if repository_names:
                return {
                    "status": 1,
                    "repositories": repository_names,
                    "path" : repository_paths
                }
            else:
                return {
                    "status": 0,
                    "message": str(errrt)
                }
        except requests.exceptions.ConnectionError as conerr:
            if repository_names:
                return {
                    "status": 1,
                    "repositories": repository_names,
                    "path" : repository_paths
                }
            else:
                return {
                    "status": 0,
                    "message": str(conerr)
                }
        except requests.exceptions.RequestException as errex:
            if repository_names:
                return {
                    "status": 1,
                    "repositories": repository_names,
                    "path" : repository_paths
                }
            else:
                return {
                    "status": 0,
                    "message": str(conerr)
                }
        page += 1
    if repository_names:
        return {
            "status": 1,
            "repositories": repository_names,
            "path" : repository_paths
        }
    else:
        return {
            "status": 0,
            "repositories": "No repositories found"
        }

def github_access_log(repository, username, headers):
    if not headers or not username or not repository:
        return {"status": 0, "error": "Missing required parameters"}
    url = f"https://api.github.com/users/{username}/events"
    filtered_events = []
    page = 1
    while True:
        try:
            response = requests.get(url, headers=headers, params={'page': page, 'per_page': 100})
            if response.status_code == 200:
                events = response.json()
                if not events:
                    break
                current_time = datetime.now(timezone.utc)
                one_hour_ago = current_time - timedelta(hours=1)
                for event in events:
                    event_time = datetime.strptime(event['created_at'], '%Y-%m-%dT%H:%M:%SZ')
                    event_time = event_time.replace(tzinfo=timezone.utc)
                    if event_time >= one_hour_ago:
                        filtered_events.append(event)
                page += 1
            else:
                return {"status": 0, "error": f"Error: {response.status_code} - {response.text}"}
        except requests.exceptions.HTTPError as errh:
            if filtered_events:
                return {
                    'status': 1,
                    "access_log": filtered_events
                }
            else:
                return {
                    "status": 0,
                    "message": str(errh)
                }
        except requests.exceptions.ReadTimeout as errrt:
            if filtered_events:
                return {
                    'status': 1,
                    "access_log": filtered_events
                }
            else:
                return {
                    "status": 0,
                    "message": str(errrt)
                }
        except requests.exceptions.ConnectionError as conerr:
            if filtered_events:
                return {
                    'status': 1,
                    "access_log": filtered_events
                }
            else:
                return {
                    "status": 0,
                    "message": str(conerr)
                }
        except requests.exceptions.RequestException as errex:
            if filtered_events:
                return {
                    'status': 1,
                    "access_log": filtered_events
                }
            else:
                return {
                    "status": 0,
                    "message": str(errex)
                } 
    if filtered_events:
        return {
            "status": 1,
            "access_log": filtered_events,
        }
    else:
        return {
            "status": 0,
            "access_log": "No recent events found",
        }

def ddr_send_rulebuilder(logs,log_type):
    if logs:
        url = "http://localhost:5050/plugin/send-to-rulebuilder"
        data = {"logs": logs, "log_type":log_type }
        headers = {
            'accept': 'application/json'
        }
        try:
            response = requests.post(url, headers=headers,json=data)
            if response.status_code == 200:
                res = {
                    "status": 1,
                    "data": response.text
                }
                return res
            else:
                return response.text
        except requests.exceptions.HTTPError as errh:
            res = {
                "status": 0,
                "message": str(errh)
            }
            return res
        except requests.exceptions.ReadTimeout as errrt:
            res = {
                "status": 0,
                "message": str(errrt)
            }
            return res
        except requests.exceptions.ConnectionError as conerr:
            res = {
                "status": 0,
                "message": str(conerr)
            }
            return res
        except requests.exceptions.RequestException as errex:
            res = {
                "status": 0,
                "message": str(errex)
            }
            return res
    else:
        res = {
            "status": 0,
            "message": "Data is not Found"
        }
        return res        

def ddr_insert_parquet(table_list,schema,save_path,partition_cols):
    if schema:
        url = "http://localhost:5050/plugin/write-parquet"
        data = {
            "table_list": table_list, 
            "parq_schema":schema ,
            "save_path": save_path, 
            "partition_cols": partition_cols
        }
        headers = {
            'accept': 'application/json'
        }
        response = requests.post(url, headers = headers,json = data)
        try:
            response = requests.post(url, headers=headers,json=data)
            if response.status_code == 200:
                res = {
                    "status": 1,
                    "data": response.text
                }
                return res
            else:
                return response.text
        except requests.exceptions.HTTPError as errh:
            res = {
                "status": 0,
                "message": str(errh)
            }
            return res
        except requests.exceptions.ReadTimeout as errrt:
            res = {
                "status": 0,
                "message": str(errrt)
            }
            return res
        except requests.exceptions.ConnectionError as conerr:
            res = {
                "status": 0,
                "message": str(conerr)
            }
            return res
        except requests.exceptions.RequestException as errex:
            res = {
                "status": 0,
                "message": str(errex)
            }
            return res
    else:
        res = {
            "status": 0,
            "message": "Data is not Found"
        }
        return res
    
def write_to_parquet(parseDataList, save_path, typingMappingParquet, partition_cols):
    if parseDataList:
        if save_path:
            ddr_insert_parquet(parseDataList, typingMappingParquet, save_path, partition_cols)
            print("Successfully written to parquet")
        else:
            print("No valid log type selected for saving.")
    
def access_log_main(data : dict) -> dict:
    if data:
        logs = data.get('access_logs', '')
        check_uid = data.get('uid', '')
        check_token = data.get('token', '') 
        if check_uid:
            token = commonFunction_obj.decrypt_value(str(check_uid), str(check_token))
        else:
            token = data.get('token', '')     
        username = data.get('username', '')   
        organization_name = data.get('organization_name', '') 
        headers = {
            'Authorization': f'token {token}',
            'Accept': 'application/vnd.github.v3+json',
        }
        access_log_data = []
        organization_repositories = get_github_repositories(headers, username)
        
        if organization_repositories.get('status', '') == 1:
            repository_list = organization_repositories.get('repositories', [])
            repository_path = organization_repositories.get('path', [])
            parseData = {}
            # audit_log_data = []
            for repository in repository_list:
                if repository:
                    access_log_history = github_access_log(repository, username, headers)
                   
                    if access_log_history and access_log_history.get('status') == 1:
                        access_logs = access_log_history.get('access_log', '')
                        
                        parse_data_list = []
                        for access_log in access_logs:
                            actor = access_log.get('actor','')
                            repo = access_log.get('repo','')
                            payload = access_log.get('payload', '')
                            event_type = access_log.get('type', '')
                            parseData['commit_data_json'] = ''
                            parseData['payload_push_id'] = '' 
                            parseData['payload_size'] = ''
                            parseData['commit_comment_id'] = ''
                            parseData['commit_comment_body'] = ''
                            parseData['create_ref'] = ''
                            parseData['create_ref_type']  = ''
                            parseData['create_description'] = ''
                            parseData['create_pusher_type'] = ''
                            parseData['delete_ref'] = ''
                            parseData['delete_ref_type'] = ''
                            parseData['fork_id'] = ''
                            parseData['fork_full_name'] = ''
                            parseData['fork_name'] = ''
                            parseData['fork_description'] = ''
                            parseData['fork_created_at'] = ''
                            parseData['fork_updated_at'] = ''
                            parseData['fork_pushed_at'] = ''
                            parseData['fork_size'] = ''
                            parseData['fork_visibility'] = ''
                            parseData['fork_owner_login'] = ''
                            parseData['fork_owner_id'] = ''
                            parseData['fork_owner_html_url'] = ''
                            parseData['fork_owner_type'] = ''
                            parseData['fork_owner_user_view_type'] = ''
                            parseData['pr_id'] = ''
                            parseData['pr_action'] = ''
                            parseData['release_id'] = ''
                            parseData['release_name'] = ''
                            parseData['watch_action'] = ''
                            if event_type == 'PushEvent':
                                if payload:
                                    commits = payload.get('commits', [])
                                    parseData['commit_data_json'] = json.dumps(commits, indent = 4)
                                    parseData['payload_push_id'] = str(payload.get('push_id', '')) if payload.get('push_id') else ''
                                    parseData['payload_size'] = str(payload.get('size', '')) if payload.get('size') else ''
                            elif event_type == 'CommitCommentEvent':
                                if payload:
                                    parseData['commit_comment_id'] = str(payload.get('comment_id', '')) if payload.get('comment_id') else ''
                                    parseData['commit_comment_body'] = str(payload.get('body', '')) if payload.get('body') else ''
                            elif event_type == 'CreateEvent':
                                if payload:
                                    parseData['create_ref'] = str(payload.get('ref', '')) if payload.get('ref') else ''
                                    parseData['create_ref_type'] = str(payload.get('ref_type', '')) if payload.get('ref_type') else ''
                                    parseData['create_description'] = str(payload.get('description', '')) if payload.get('description') else ''
                                    parseData['create_pusher_type'] = str(payload.get('pusher_type', '')) if payload.get('pusher_type') else ''
                            elif event_type == 'DeleteEvent':
                                if payload:
                                    parseData['delete_ref'] = str(payload.get('ref', '')) if payload.get('ref') else ''
                                    parseData['delete_ref_type'] = str(payload.get('ref_type', '')) if payload.get('ref_type') else ''
                            elif event_type == 'ForkEvent':
                                if payload:
                                    forkee = payload.get('forkee', {})
                                    parseData['fork_id'] = str(forkee.get('id', '')) if forkee.get('id') else ''
                                    parseData['fork_full_name'] = str(forkee.get('full_name', '')) if forkee.get('full_name') else ''
                                    parseData['fork_name'] = str(forkee.get('name', '')) if forkee.get('name') else ''  
                                    parseData['fork_description'] = str(forkee.get('description', '')) if forkee.get('description') else ''
                                    parseData['fork_created_at'] = str(forkee.get('created_at', '')) if forkee.get('created_at') else '' 
                                    parseData['fork_updated_at'] = str(forkee.get('updated_at', '')) if forkee.get('updated_at') else '' 
                                    parseData['fork_pushed_at'] = str(forkee.get('pushed_at', '')) if forkee.get('pushed_at') else '' 
                                    parseData['fork_size'] = str(forkee.get('size', '')) if forkee.get('size') else ''    
                                    parseData['fork_visibility'] = str(forkee.get('visibility', '')) if forkee.get('visibility') else '' 
                                    owner = forkee.get('owner', {})
                                    parseData['fork_owner_login'] = str(owner.get('login', '')) if owner.get('login') else ''  
                                    parseData['fork_owner_id'] = str(owner.get('id', '')) if owner.get('id') else ''  
                                    parseData['fork_owner_html_url'] = str(owner.get('html_url', '')) if owner.get('html_url') else ''
                                    parseData['fork_owner_type'] = str(owner.get('type', '')) if owner.get('type') else ''
                                    parseData['fork_owner_user_view_type'] = str(owner.get('user_view_type', '')) if owner.get('user_view_type') else ''
                            elif event_type == 'PullRequestEvent':
                                if payload:
                                    parseData['pr_id'] = str(payload.get('pull_request', {}).get('id', '')) if payload.get('pull_request') else ''
                                    parseData['pr_action'] = str(payload.get('action', '')) if payload.get('action') else ''   
                            elif event_type == 'ReleaseEvent':
                                if payload:
                                    parseData['release_id'] = str(payload.get('release', {}).get('id', '')) if payload.get('release') else ''
                                    parseData['release_name'] = str(payload.get('release', {}).get('name', '')) if payload.get('release') else ''
                            elif event_type == 'WatchEvent':
                                if payload:
                                    parseData['watch_action'] = str(payload.get('action', '')) if payload.get('action') else ''            
                            event = json.dumps(access_log, indent = 4)                           
                            parseData['raw_message'] = str(event) if event else ""
                            parseData['login_id'] = str(access_log.get('id','')) if access_log.get('id') else ''
                            parseData['login_type'] = str(access_log.get('type')) if access_log.get('type') else ''
                            parseData['user_name'] = str(actor.get('login', '')) if actor.get('login') else ''
                            parseData['url'] = str(actor.get('url', '')) if actor.get('url') else ''
                            parseData['repo_id'] = str(repo.get('id', '')) if repo.get('id') else ''
                            parseData['repo_name'] = str(repo.get('name', '')) if repo.get('name') else ''
                            parseData['repo_url'] = str(repo.get('url', '')) if repo.get('url') else ''
                            parseData['payload_push_id'] = str(payload.get('push_id', '')) if payload.get('push_id') else ''
                            parseData['payload_size'] = str(payload.get('size', '')) if payload.get('size') else ''
                            parseData['access_log_public'] = str(access_log.get('public', '')) if access_log.get('public') else ''
                            parseData['access_log_created_at'] = str(access_log.get('created_at', '')) if access_log.get('created_at') else ''

                            local_timezone = get_localzone()
                            timestamp = access_log.get('created_at')
                            event_time_utc = datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%SZ')
                            utc_zone = pytz.utc
                            event_time_utc = utc_zone.localize(event_time_utc)
                            event_time_local = event_time_utc.astimezone(local_timezone)
                            log_date = event_time_local.date()
                            log_hour = event_time_local.hour
                            log_date_str = log_date.strftime('%Y-%m-%d')
                            epoch_time = event_time_local.timestamp()
                            parseData["timeepoch"] = epoch_time 
                            dt = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%SZ")
                            parseData["date"] = log_date_str  
                            parseData["log_hour"] = log_hour
                            parseData['resource_name'] = ''
                            parseData['resouce_path'] = ''
                            parseData['resource'] = 'file'

                            if LSHashing_enabled:
                                try:
                                    if logs == 1:
                                        log_hash = LSHashing.generate_hash(json.dumps(parseData), "Github Access Event Logs")
                                    else:
                                        log_hash = ""
                                except:
                                    log_hash = ""
                            else:
                                log_hash = ""
                            parseData["ls_uid"] = log_hash 
                            access_log_data.append(parseData)                    
        if logs == 1:
            log_category='file_access'
            save_path="/data/datastore/sqlite3/plugins/" + str(pluginName) + "/access_logs"
            if log_category and save_path:
                ddr_send_rulebuilder(access_log_data,log_category) 
                typingMappingParquet = {
                    "raw_message": "string", 
                    "commit_data_json": "string", 
                    "payload_push_id": "string", 
                    "payload_size": "string", 
                    "commit_comment_id": "string", 
                    "commit_comment_body": "string", 
                    "create_ref": "string",
                    "create_ref_type": "string", 
                    "create_description":"string",
                    "create_pusher_type":"string",
                    "delete_ref": "string", 
                    "delete_ref_type": "string", 
                    "fork_id": "string", 
                    "fork_full_name": "string", 
                    "fork_name": "string", 
                    "fork_description":"string",
                    "fork_created_at": "string", 
                    "fork_updated_at": "string", 
                    "fork_pushed_at": "string", 
                    "fork_size": "string", 
                    "fork_visibility": "string",
                    "fork_owner_login": "string",
                    "fork_owner_id": "string",
                    "fork_owner_html_url": "string",
                    "fork_owner_type": "string",
                    "fork_owner_user_view_type" : "string",
                    "pr_id" : "string",
                    "pr_action" : "string",
                    "release_id" : "string",
                    "release_name" : "string",
                    "watch_action" : "string",
                    "login_id" : "string",
                    "login_type" : "string",
                    "user_name" : "string",
                    "url" : "string",
                    "repo_id" : "string",
                    "repo_name" : "string",
                    "repo_url" : "string",
                    "payload_push_id" : "string",
                    "payload_size" : "string",
                    "access_log_public" : "string",
                    "access_log_created_at" : "string",
                    "timeepoch": "int64", 
                    "log_hour": "string", 
                    "date": "string"
                }        
                partition_cols = ["date", "log_hour"]
                write_to_parquet(access_log_data, save_path, typingMappingParquet, partition_cols)

def github_audit_log( organization_name, headers):
    if headers and organization_name:
        url = f'https://api.github.com/orgs/{organization_name}/audit-log'
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            audit_log = response.json()
            if audit_log:    
                current_time = datetime.now(timezone.utc)
                one_hour_ago = current_time - timedelta(hours=1)
                filtered_audit_log = []
                for event in audit_log:
                    event_time = datetime.strptime(event['@timestamp'], '%Y-%m-%dT%H:%M:%SZ')
                    if event_time >= one_hour_ago:
                        filtered_audit_log.append(event)
                res = {
                    "status": 1,
                    "audit_log": filtered_audit_log,
                    }
            else:
                res = {
                    "status": 0,
                    "audit_log": "No audit log found",
                }
        else:
            res = {
                "status": 0,
                "audit_log": "Username or token is invalid input",
            }
        return res

def audit_log_main(data):
    if data:
        check_uid = data.get('uid', '')
        check_token = data.get('token', '') 

        if check_uid:
            token = commonFunction_obj.decrypt_value(str(check_uid), str(check_token))
        else:
            token = data.get('token', '') 
        logs = data.get('audit_logs', '')    
        username = data.get('username', '')   
        organization_name = data.get('organization_name', '') 
        headers = {
            'Authorization': f'token {token}',
            'Accept': 'application/vnd.github.v3+json',
        }
        audit_log_data = []
       
        organization_repositories = get_github_repositories(headers, username)
        
        if organization_repositories.get('status', '') == 1:
            repository_list = organization_repositories.get('repositories', [])
            parse_data_list = []
            parseData = {}
            for repository in repository_list:
                if repository:
                    audit_log_history = github_audit_log( username, headers)
                    if audit_log_history and audit_log_history.get('status') == 1:
                        audit_logs = audit_log_history.get('audit_log', '')
                        for audit_log in audit_logs:
                            actor_location = audit_log.get('actor_location', '')
                            parseData['timestamp'] = ''
                            parseData['document_id'] = ''
                            parseData['action'] = ''
                            parseData['actor'] = ''
                            parseData['actor_id'] = ''
                            parseData['actor_is_bot'] = ''
                            parseData['actor_location'] = ''
                            parseData['country_code'] = ''
                            parseData['business'] = ''
                            parseData['business_id'] = ''
                            parseData['created_at'] = ''
                            parseData['external_identity_nameid'] = ''
                            parseData['external_identity_username'] = ''
                            parseData['operation_type'] = ''
                            parseData['organizations'] = ''
                            parseData['org_id'] = ''
                            parseData['permission'] = ''
                            parseData['request_access_security_header'] = ''
                            parseData['user'] = ''
                            parseData['user_agent'] = ''
                            parseData['user_id'] = ''
                            parseData['hashed_token'] = ''
                            parseData['oauth_credential_type'] = ''
                            parseData['token_id'] = ''
                            parseData['token_scopes'] = ''
                            parseData['public_repo'] = ''
                            parseData['repo'] = ''
                            parseData['repo_id'] = ''
                            parseData['request_category'] = ''
                            parseData['visibility'] = ''
                            parseData['timestamp'] = str(audit_log.get('@timestamp','')) if audit_log.get('@timestamp') else ''
                            parseData['document_id'] = str(audit_log.get('_document_id', '')) if audit_log.get('_document_id') else ''
                            parseData['action'] = str(audit_log.get('action', '')) if audit_log.get('action') else ''
                            parseData['actor'] = str(audit_log.get('actor', '')) if audit_log.get('actor') else ''  
                            parseData['actor_id'] = str(audit_log.get('actor_id', ''))  if audit_log.get('actor_id') else ''
                            parseData['actor_is_bot'] = str(audit_log.get('actor_is_bot', ''))  if audit_log.get('actor_is_bot') else ''
                            parseData['country_code'] = str(actor_location.get('country_code', '')) if actor_location.get('country_code') else ''
                            parseData['business'] = str(audit_log.get('business', '')) if audit_log.get('business') else ''
                            parseData['business_id'] = str(audit_log.get('business_id', '')) if audit_log.get('business_id') else ''
                            parseData['created_at'] = str(audit_log.get('created_at', '')) if audit_log.get('created_at') else ''
                            parseData['external_identity_nameid'] = str(audit_log.get('external_identity_nameid', '')) if audit_log.get('external_identity_nameid') else ''
                            parseData['external_identity_username'] = str(audit_log.get('external_identity_username', '')) if audit_log.get('external_identity_username') else ''
                            parseData['operation_type'] = str(audit_log.get('operation_type', '')) if audit_log.get('operation_type') else ''
                            parseData['organizations'] = str(audit_log.get('org', '')) if audit_log.get('org') else ''
                            parseData['org_id'] = str(audit_log.get('org_id', '')) if audit_log.get('org_id') else ''
                            parseData['permission'] = str(audit_log.get('permission', '')) if audit_log.get('permission') else ''
                            parseData['request_access_security_header'] = str(audit_log.get('request_access_security_header', '')) if audit_log.get('request_access_security_header') else ''
                            parseData['user'] = str(audit_log.get('user', '')) if audit_log.get('user') else ''
                            parseData['user_agent'] = str(audit_log.get('user_agent', '')) if audit_log.get('user_agent') else ''
                            parseData['user_id'] = str(audit_log.get('user_id', '')) if audit_log.get('user_id') else ''
                            parseData['hashed_token'] = str(audit_log.get('hashed_token', '')) if audit_log.get('hashed_token') else '' 
                            parseData['oauth_credential_type'] = str(audit_log.get('oauth_credential_type', '')) if audit_log.get('oauth_credential_type') else ''
                            parseData['token_id'] = str(audit_log.get('token_id', '')) if audit_log.get('token_id') else ''
                            parseData['token_scopes'] = str(audit_log.get('token_scopes', '')) if audit_log.get('token_scopes') else ''
                            parseData['public_repo'] = str(audit_log.get('public_repo', '')) if audit_log.get('public_repo') else ''
                            parseData['repo'] = str(audit_log.get('repo', '')) if audit_log.get('repo') else ''
                            parseData['repo_id'] = str(audit_log.get('repo_id', '')) if audit_log.get('repo_id') else ''
                            parseData['request_category'] = str(audit_log.get('request_category', '')) if audit_log.get('request_category') else ''
                            parseData['visibility'] = str(audit_log.get('visibility','')) if audit_log.get('visibility') else ''

                            local_timezone = get_localzone()
                            timestamp = audit_log.get('@timestamp', '')
                            event_time_utc = datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%SZ')
                            utc_zone = pytz.utc
                            event_time_utc = utc_zone.localize(event_time_utc)
                            event_time_local = event_time_utc.astimezone(local_timezone)
                            log_date = event_time_local.date()
                            log_hour = event_time_local.hour
                            log_date_str = log_date.strftime('%Y-%m-%d')
                            epoch_time = event_time_local.timestamp()
                            parseData["date"] = log_date_str  
                            parseData["log_hour"] = log_hour
                            parseData["timeepoch"] = int(epoch_time)
                            audits = json.dumps(audit_log, indent = 4)                           
                            parseData['raw_message'] = str(audits) if audits else ""
                            if LSHashing_enabled:
                                try:
                                    if logs == 1:
                                        log_hash = LSHashing.generate_hash(json.dumps(parseData), "Github Audit Logs")
                                    else:
                                        log_hash = ""
                                except:
                                    log_hash = ""
                            else:
                                log_hash = ""
                            parseData["ls_uid"] = log_hash 
                            audit_log_data.append(parseData)              
        if logs == 1:
            log_category='file_access'
            save_path="/data/datastore/sqlite3/plugins/" + str(pluginName) + "/audit_logs"
            if log_category and save_path:
                ddr_send_rulebuilder(audit_log_data,log_category) 
                typingMappingParquet = {
                    "raw_message": "string", 
                    "document_id": "string", 
                    "action": "string", 
                    "actor": "string", 
                    "actor_id": "string", 
                    "actor_is_bot": "string", 
                    "country_code": "string",
                    "business": "string", 
                    "business_id":"string",
                    "created_at":"string",
                    "external_identity_nameid": "string", 
                    "external_identity_username": "string", 
                    "operation_type": "string", 
                    "organizations": "string", 
                    "org_id": "string", 
                    "permission":"string",
                    "request_access_security_header": "string", 
                    "user": "string", 
                    "user_agent": "string", 
                    "user_id": "string", 
                    "hashed_token": "string",
                    "oauth_credential_type": "string",
                    "token_id": "string",
                    "token_scopes": "string",
                    "public_repo": "string",
                    "repo" : "string",
                    "repo_id" : "string",
                    "visibility" : "string",
                    "timeepoch": "int64", 
                    "log_hour": "string", 
                    "date": "string"
                }        
                partition_cols = ["date", "log_hour"]   
                write_to_parquet(audit_log_data,save_path, typingMappingParquet, partition_cols)                                    
def log_main():
    settings = get_settings_data()
    for setting in settings:
        access_log_main(setting)
        audit_log_main(setting)
        
def initiate():
    try:
        log_main()
        status_json = read_json(statsPath)
        status_json["AccessLogGitHubCronRuntime"] = str(datetime.now())
        write_json(statsPath, status_json)
        print ("completed everything, including updating last cron runtime")
    except Exception as ex:
        print(ex)
 
 
if __name__ == "__main__":
    initiate()