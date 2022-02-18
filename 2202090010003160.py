#!/usr/bin/python

import sys
import hdinsight_common.ClusterManifestParser as ClusterManifestParser
import subprocess
import os
import base64
import time
import requests
import json
import shlex
import socket


class buildpatch():
    def __init__(self):
        self.hn0 = list(socket.getfqdn())
        self.hn1 = list(socket.getfqdn())
        self.hn0[2] = '0'
        self.hn1[2] = '1'

        self.manifest = ClusterManifestParser.parse_local_manifest()
        self.cred = base64.b64decode(self.manifest.ambari_users.usersmap.get('hdinsightwatchdog').password)
        self.clustername = self.manifest.deployment.cluster_name
        self.baseurl = 'http://headnodehost:8080/api/v1/clusters/' + self.clustername
        self.ambari_user = 'hdinsightwatchdog'

    def restart(self):

        headnodes = []
        wknodes = []
        s = requests.Session()
        # s.headers.clear()
        s.auth = (self.ambari_user, self.cred)

        enumhosts = s.get(self.baseurl)
        hosts = enumhosts.json()['hosts']
        for list in range(len(hosts)):
            if hosts[list]['Hosts']['host_name'].startswith('wn'):
                wknodes.append(hosts[list]['Hosts']['host_name'])
            elif hosts[list]['Hosts']['host_name'].startswith('hn'):
                headnodes.append(hosts[list]['Hosts']['host_name'])


        headnodes.sort()
        allnodes = ','.join(headnodes)+ ',' + ','.join(wknodes)
        bothheadnodes = ','.join(headnodes)
        s = requests.Session()
        s.headers.clear()
        s.auth = (self.ambari_user, self.cred)
        reboot_url = self.baseurl + '/requests'

        requestinfo3 = '{"RequestInfo":{"command":"RESTART","context":"ICM:275711412 JDBC URL Patch - Restarting Spark...","operation_level":{"level":"SERVICE","cluster_name":"' + self.clustername + '","service_name":"SPARK2"}},"Requests/resource_filters":[{"service_name":"SPARK2","component_name":"LIVY2_SERVER","hosts":"' + headnodes[0] + '"},{"service_name":"SPARK2","component_name":"SPARK2_CLIENT","hosts":"' + allnodes + '"},{"service_name":"SPARK2","component_name":"SPARK2_JOBHISTORYSERVER","hosts":"' + headnodes[0] + '"},{"service_name":"SPARK2","component_name":"SPARK2_THRIFTSERVER","hosts":"' + bothheadnodes + '"}]}'

        payload = requestinfo3
        s.headers.update({'X-Requested-By': 'ICM-275711412'})

        r = s.post(reboot_url, payload)
        print(r.status_code)

    def currentconfig(self):

        zknodes = []
        s = requests.Session()
        # s.headers.clear()
        s.auth = (self.ambari_user, self.cred)

        enumhosts = s.get(self.baseurl)
        hosts = enumhosts.json()['hosts']
        for list in range(len(hosts)):
            if hosts[list]['Hosts']['host_name'].startswith('zk'):
                zknodes.append(hosts[list]['Hosts']['host_name'])


        s = requests.Session()
        s.auth = (self.ambari_user, self.cred)
        s.headers.update({'X-Requested-By': 'ICM-275711412'})
        r = s.get(self.baseurl + '?fields=Clusters/desired_configs/spark2-defaults')
        tag = r.json()['Clusters']['desired_configs']['spark2-defaults']['tag']
        r = s.get(self.baseurl + '/configurations?type=spark2-defaults&tag=' + tag)
        config_old = r.json()['items'][0]
        config_new = r.json()['items'][0]

        zkstring = 'jdbc:hive2://' + zknodes[0] + ':2181,' + zknodes[1] + ':2181,' + zknodes[
            2] + ':2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2'
        config_new['properties']['spark.sql.hive.hiveserver2.jdbc.url'] = zkstring
        config_new['tag'] = 'version' + str(int(round(time.time() * 1000000000)))
        del config_new['Config']
        del config_new['href']
        del config_new['version']
        config_new = {"Clusters": {"desired_config": config_new}}

        body = config_new
        r = s.put(self.baseurl, data=json.dumps(body))
        print(r.url)
        print(r.status_code)
        assert r.status_code == 200

        r = s.get(self.baseurl + '?fields=Clusters/desired_configs/spark2-thrift-sparkconf')
        tag = r.json()['Clusters']['desired_configs']['spark2-thrift-sparkconf']['tag']
        r = s.get(self.baseurl + '/configurations?type=spark2-thrift-sparkconf&tag=' + tag)
        config_old = r.json()['items'][0]
        config_new = r.json()['items'][0]
        zkstringthrift = zkstring + ';hive.server2.proxy.user=${user}'

        config_new['properties']['spark.sql.hive.hiveserver2.jdbc.url'] = zkstringthrift
        config_new['tag'] = 'version' + str(int(round(time.time() * 1000000000)))
        del config_new['Config']
        del config_new['href']
        del config_new['version']
        config_new = {"Clusters": {"desired_config": config_new}}

        body = config_new
        r = s.put(self.baseurl, data=json.dumps(body))
        print(r.url)
        print(r.status_code)
        assert r.status_code == 200

def headnodehost():
    return os.path.exists('/var/run/ambari-server/ambari-server.pid')


def main():
    if not headnodehost():
        sys.exit()

    patch = buildpatch()
    patch.currentconfig()
    patch.restart()


if __name__ == '__main__':
    main()
