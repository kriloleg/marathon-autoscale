__author__ = 'tkraus'

import sys
import requests
import json
import math
import time
import os

dcos_master = os.environ['AUTOSCALE_MARATHON_HOST']
auth_enabled = os.getenv('AUTOSCALE_AUTH_ENABLED', 'false')
userid = os.getenv('AUTOSCALE_USER_ID', '')
password = os.getenv('AUTOSCALE_USER_PASSWORD', '')
marathon_app = os.environ['AUTOSCALE_APP']
max_mem_percent = int(os.environ['AUTOSCALE_MAX_MEM'])
max_cpu_time = int(os.environ['AUTOSCALE_MAX_CPU'])
trigger_mode = os.environ['AUTOSCALE_TRIGGER']
max_instances = int(os.environ['AUTOSCALE_MAX_INSTANCES'])
autoscale_multiplier = float(os.environ['AUTOSCALE_MULITPLIER'])
scale_back_multiplier = 0.5

cpu_revert_coef=20
memory_revert_coef=20

cpu_to_revert=max_cpu_time - cpu_revert_coef
memory_to_revert=max_mem_percent - memory_revert_coef

class Marathon(object):
    def __init__(self, dcos_master,dcos_auth_token):
        self.name = dcos_master
        self.uri=(dcos_master)
        self.headers={'Authorization': 'token='+dcos_auth_token, 'Content-type': 'application/json'}
        self.apps = self.get_all_apps()

    def get_all_apps(self):
        response = requests.get(self.uri + '/v2/apps', headers=self.headers, verify=False).json()
        if response['apps'] ==[]:
            print ("No Apps found on Marathon")
            sys.exit(1)
        else:
            apps=[]
            for i in response['apps']:
                appid = i['id'].strip('/')
                apps.append(appid)
            print ("Found the following App LIST on Marathon =", apps)
            return apps

    def get_app_details(self, marathon_app):
        response = requests.get(self.uri + '/v2/apps/'+ marathon_app, headers=self.headers, verify=False).json()
        if (response['app']['tasks'] ==[]):
            print ('No task data on Marathon for App !', marathon_app)
        else:
            app_instances = response['app']['instances']
            self.appinstances = app_instances
            print(marathon_app, "has", self.appinstances, "deployed instances")
            app_task_dict={}
            for i in response['app']['tasks']:
                taskid = i['id']
                hostid = i['host']
                slaveId=i['slaveId']
                print ('DEBUG - taskId=', taskid +' running on '+hostid + 'which is Mesos Slave Id '+slaveId)
                app_task_dict[str(taskid)] = [str(slaveId), str(hostid)]
            return app_task_dict

    def scale_app(self,marathon_app,autoscale_multiplier):
        target_instances_float=self.appinstances * autoscale_multiplier
        target_instances=math.ceil(target_instances_float)
        if (target_instances > max_instances):
            print("Reached the set maximum instances of", max_instances)
            target_instances=max_instances
        else:
            target_instances=target_instances
        data ={'instances': target_instances}
        json_data=json.dumps(data)
        #headers = {'Content-type': 'application/json'}
        response=requests.put(self.uri + '/v2/apps/'+ marathon_app, json_data,headers=self.headers,verify=False)
        print ('Scale_app return status code =', response.status_code)

def dcos_auth_login(dcos_master,userid,password):
    '''
    Will login to the DCOS ACS Service and RETURN A JWT TOKEN for subsequent API requests to Marathon, Mesos, etc
    '''
    rawdata = {'uid' : userid, 'password' : password}
    login_headers={'Content-type': 'application/json'}
    response = requests.post(dcos_master + '/acs/api/v1/auth/login', headers=login_headers, data=json.dumps(rawdata),verify=False).json()
    auth_token=response['token']
    return auth_token

def get_task_agentstatistics(task, agent, host):
    # Get the performance Metrics for all the tasks for the Marathon App specified
    # by connecting to the Mesos Agent and then making a REST call against Mesos statistics
    # Return to Statistics for the specific task for the marathon_app
    dcos_headers={'Authorization': 'token='+dcos_auth_token, 'Content-type': 'application/json'}
    response = requests.get('http://' + host + ':5051' + '/monitor/statistics.json', verify=False, headers=dcos_headers, allow_redirects=True).json()
    print(response)
    # print ('DEBUG -- Getting Mesos Metrics for Mesos Agent =',agent)
    for i in response:
        executor_id = i['executor_id']
        #print("DEBUG -- Printing each Executor ID ", executor_id)
        if (executor_id == task):
            task_stats = i['statistics']
            print ('****Specific stats for task',executor_id,'=',task_stats)
            return task_stats
def timer():
    print("Successfully completed a cycle, sleeping for 30 seconds ...")
    time.sleep(30)
    return

if __name__ == "__main__":

    if auth_enabled == 'true':
        print ("This application tested with Python3 only")
        dcos_auth_token=dcos_auth_login(dcos_master,userid,password)
        print('Auth Token is = ' + dcos_auth_token)
    else:
        dcos_auth_token=''

    running=1
    while running == 1:
        # Initialize the Marathon object
        aws_marathon = Marathon(dcos_master,dcos_auth_token)
        print ("Marathon URI = ...", aws_marathon.uri)
        print ("Marathon Headers = ...", aws_marathon.headers)
        print ("Marathon name = ...", aws_marathon.name)
        # Call get_all_apps method for new object created from aws_marathon class and return all apps
        marathon_apps = aws_marathon.get_all_apps()
        print ("The following apps exist in Marathon...", marathon_apps)
        # Quick sanity check to test for apps existence in MArathon.
        if (marathon_app in marathon_apps):
            print ("  Found your Marathon App=", marathon_app)
        else:
            print ("  Could not find your App =", marathon_app)
            sys.exit(1)
        # Return a dictionary comprised of the target app taskId and hostId.
        app_task_dict = aws_marathon.get_app_details(marathon_app)
        print ("    Marathon  App 'tasks' for", marathon_app, "are=", app_task_dict)

        app_cpu_values = []
        app_mem_values = []
        for task, data in app_task_dict.items():
            #cpus_time =(task_stats['cpus_system_time_secs']+task_stats['cpus_user_time_secs'])
            #print ("Combined Task CPU Kernel and User Time for task", task, "=", cpus_time)
            agent = data[0]
            host = data[1]

            print('Task = '+ task)
            print ('Agent = ' + agent)
            print ('Host = ' + host)

            # Compute CPU usage
            task_stats = get_task_agentstatistics(task, agent, host)
            if task_stats != None:
                cpus_system_time_secs0 = float(task_stats['cpus_system_time_secs'])
                cpus_user_time_secs0 = float(task_stats['cpus_user_time_secs'])
                timestamp0 = float(task_stats['timestamp'])
            else:
                cpus_system_time_secs0 = 0
                cpus_user_time_secs0 = 0
                timestamp0 = 0

            time.sleep(1)

            task_stats = get_task_agentstatistics(task, agent, host)
            if task_stats != None:
                cpus_system_time_secs1 = float(task_stats['cpus_system_time_secs'])
                cpus_user_time_secs1 = float(task_stats['cpus_user_time_secs'])
                timestamp1 = float(task_stats['timestamp'])
            else:
                cpus_system_time_secs0 = 0
                cpus_user_time_secs0 = 0
                timestamp0 = 0

            cpus_time_total0 = cpus_system_time_secs0 + cpus_user_time_secs0
            cpus_time_total1 = cpus_system_time_secs1 + cpus_user_time_secs1
            cpus_time_delta = cpus_time_total1 - cpus_time_total0
            timestamp_delta = timestamp1 - timestamp0

            # CPU percentage usage
            usage = float(cpus_time_delta / timestamp_delta) * 100

            # RAM usage
            if task_stats != None:
                mem_rss_bytes = int(task_stats['mem_rss_bytes'])
                mem_limit_bytes = int(task_stats['mem_limit_bytes'])
                mem_utilization = 100 * (float(mem_rss_bytes) / float(mem_limit_bytes))

            else:
                mem_rss_bytes = 0
                mem_limit_bytes = 0
                mem_utilization = 0


            print()
            print ("task", task, "mem_rss_bytes=", mem_rss_bytes)
            print ("task", task, "mem Utilization=", mem_utilization)
            print ("task", task, "mem_limit_bytes=", mem_limit_bytes)
            print()

            #app_cpu_values.append(cpus_time)
            app_cpu_values.append(usage)
            app_mem_values.append(mem_utilization)
        # Normalized data for all tasks into a single value by averaging
        app_avg_cpu = (sum(app_cpu_values) / len(app_cpu_values))
        print ('Current Average  CPU Time for app', marathon_app, '=', app_avg_cpu)
        app_avg_mem=(sum(app_mem_values) / len(app_mem_values))
        print ('Current Average Mem Utilization for app', marathon_app,'=', app_avg_mem)
        #Evaluate whether an autoscale trigger is called for
        print('\n')
        if (trigger_mode == "and"):
            if (app_avg_cpu > max_cpu_time) and (app_avg_mem > max_mem_percent):
                print ("Autoscale triggered based on 'both' Mem & CPU exceeding threshold")
                aws_marathon.scale_app(marathon_app, autoscale_multiplier)
            elif (app_avg_cpu < cpu_to_revert) and (app_avg_mem < memory_to_revert):
                print ("Autoscale triggered based on 'both' Mem & CPU exceeding threshold. Reverting")
                aws_marathon.scale_app(marathon_app, scale_back_multiplier)
            else:
                print ("Both values were not greater than autoscale targets")
        elif (trigger_mode == "or"):
            if (app_avg_cpu > max_cpu_time) or (app_avg_mem > max_mem_percent):
                print ("Autoscale triggered based Mem 'or' CPU exceeding threshold")
                aws_marathon.scale_app(marathon_app, autoscale_multiplier)
            elif (app_avg_cpu < cpu_to_revert) or (app_avg_mem < memory_to_revert):
                print ("Autoscale triggered based Mem 'or' CPU exceeding threshold. Reverting") 
                aws_marathon.scale_app(marathon_app, scale_back_multiplier)
            else:
                print ("Neither Mem 'or' CPU values exceeding threshold")
        timer()
