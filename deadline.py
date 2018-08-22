#!/usr/bin/env python
# -*- encoding: utf-8 -*-

__author__ = 'LeonWang'

from Deadline.DeadlineConnect import DeadlineCon as Connect
import time
import json
from datetime import datetime

'''
To use deadline standlone python api,you should
1. copy the "Deadline" folder containing the Standlone Python API from \\your\repository\api\python to the "site-packages" folder of your python installation;
2. set the web service listening SERVER_PORT in "Deadline Monitor-->Tools-->Configure Repository Options-->Web Service Settings-->Listening Port",by default it`s 8080;
3. in your Deadline client installation folder \\Thinkbox\Deadline\bin you will find deadlinewebservice.exe launch it and you are ready to use deadline api.
'''

SERVER_IP = '192.168.61.79' #SERVER_IP for the web service
SERVER_PORT = 8082 #the web service listening SERVER_PORT
POOL_CONF = {"bcs1":"none",   #imitate config file
               "bcs2":"test",
               "bcs3":"wym",
               "bcs4":"test2"
               }

class LEON_DEADLINE(object):
    def __init__(self,SERVER_IP = SERVER_IP,SERVER_PORT = SERVER_PORT):
        self.SERVER_IP = SERVER_IP
        self.SERVER_PORT = SERVER_PORT
        self.con = Connect(self.SERVER_IP,self.SERVER_PORT)
        
    #get pool names
    def getPoolList(self):
        pool_list = self.con.Pools.GetPoolNames()
        return pool_list
    
    #add pool
    def addPool(self,pool_name):
        self.con.Pools.AddPool(pool_name)
        return True
    
    #bcs name to pool name
    def bcsToSlaveList(self,bcs_name):
        pool_name = POOL_CONF[bcs_name]
        slave_list = self.con.Slaves.GetSlaveNamesInPool(pool_name)
        return slave_list

    #from pool name get the slave list
    def poolToSlaveList(self,pool_name = "none"):
        slave_list = self.con.Slaves.GetSlaveNamesInPool(pool_name)
        return slave_list
    
    #check slave job and store it
    def getSlaveJob(self,slave_name):
        slave_job = self.con.Slaves.GetSlaveInfo(slave_name)["JobName"]
        return slave_job
    
    #get host name from slave name
    def slaveToHost(self,slave_name):
        host_name = self.con.Slaves.GetSlaveInfo(slave_name)["Host"]
        return host_name
    
    #get IP from slave name
    def slaveToIP(self,slave_name):
        ip = self.con.Slaves.GetSlaveInfo(slave_name)["IP"]
        return ip
    
    #get slave infos
    def getSlaveListInfos(self,slave_list=[]):
        info_list = self.con.Slaves.GetSlaveInfos(slave_list)
        return info_list
    
    #delete slave
    def deleteSlave(self,slave_name):
        self.con.Slaves.DeleteSlave(slave_name)
        return True

if __name__=="__main__":
    # 1. set the POOL_CONF, connect deadline
    this_con = LEON_DEADLINE()
    
    # 2. check if all the pools in POOL_CONF exist,if not,create
    for pool in POOL_CONF.items():
        if pool[-1] not in this_con.getPoolList():
            this_con.addPool(pool[-1])
    print this_con.getPoolList()
            
    # 3. traverse all the pools in POOL_CONF, get all of the slave lists
    slave_list = []
    for bcs in POOL_CONF:
        slave_list += this_con.bcsToSlaveList(bcs)
    
    # 4. keep traverse all the slaves every 10 minutes, save slave stat and time
    while len(slave_list) > 0:
        print "start slave_list",slave_list
        file_path = r'C:\Users\wangyongmeng\Desktop\work\coding\deadline\log.json'
        input_file = open(file_path)
        try:
            old_data = json.loads(input_file.read())
        except:
            old_data = {}
        print "old_data",old_data
        input_file.close()
        temp_slave_dict = {}
        deleted_slave_list = []
        for slave in slave_list:
            print slave," running"
            slave_job = this_con.getSlaveJob(slave)
            try:
                if slave_job == "" and old_data[slave][0] == "": # if job name is empty and job name stored in json file is empty, delete this slave
                    #this_con.deleteSlave(slave)
                    deleted_slave_list.append(slave)
                else:
                    temp_slave_dict[slave] = [slave_job,datetime.now().strftime("%Y-%m-%d %H:%M:%S")]
            except:
                temp_slave_dict[slave] = [slave_job,datetime.now().strftime("%Y-%m-%d %H:%M:%S")]
        out_file = open(file_path,'w')
        out_file.write(json.dumps(temp_slave_dict))
        print json.dumps(temp_slave_dict)
        out_file.close()
        for slave in deleted_slave_list:
            slave_list.remove(slave)
        print "end slave_list",slave_list
        
        # 5. get host name from deleted_slave_list
        deleted_host_list = []
        for slave in deleted_slave_list:
            deleted_host_list.append(this_con.slaveToHost(slave))
        print "deleted_host_list",deleted_host_list
        print "###########################################\n"
        time.sleep(60)
        
    '''
    [u'none', u'test', u'wym', u'test2']
    start slave_list [u'more-td01-pc', u'render-01']
    old_data {}
    more-td01-pc  running
    render-01  running
    {"more-td01-pc": ["", "2018-08-22 17:56:13"], "render-01": ["", "2018-08-22 17:56:13"]}
    end slave_list [u'more-td01-pc', u'render-01']
    deleted_host_list []
    ###########################################
    
    start slave_list [u'more-td01-pc', u'render-01']
    old_data {u'more-td01-pc': [u'', u'2018-08-22 17:56:13'], u'render-01': [u'', u'2018-08-22 17:56:13']}
    more-td01-pc  running
    render-01  running
    {}
    end slave_list []
    deleted_host_list [u'MORE-TD01-PC', u'Render-01']
    ###########################################

    '''
    
