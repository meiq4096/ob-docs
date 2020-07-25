#!/usr/bin/env python
"""
Very simple ConfigUrl Server in python.
Usage::
    python configurl_server.py <IP> <PORT>
    python configurl_server.py 10.10.10.10 8080

Send a GET request::
    curl "http://127.0.0.1:1080/services?Action=GetObProxyConfig"
    curl "http://127.0.0.1:1080/services?Action=ObRootServiceInfo&ObRegion=obcluster"

Send a POST request::
    curl -X POST -d '{"ObRegion":"obcluster","ObRegionId": 100000, "RsList":[{"address":"100.81.181.180:2882","role":"LEADER","sql_port":2881},{"address":"100.81.181.183:2882","role":"FOLLOWER","sql_port":2881},{"address":"100.81.181.186:2882","role":"FOLLOWER","sql_port":2881}],"ReadonlyRsList":[]}' "http://127.0.0.1:1080/services?Action=ObRootServiceInfo&ObRegion=obcluster"
"""
import json
import os
import urllib
from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
from sys import argv
import threading

CODE_200_TEMP = "{{\"Code\":200,\"Cost\":1,\"Data\":{0},\"Message\":\"successful\",\"Success\":true}}"
CODE_400_TEMP = "{{\"Code\":400,\"Cost\":1,\"Data\":\"\",\"Message\":\"{0}\",\"Success\":false}}"
FILE_LOCK = threading.Lock()


class ConfigUrlServer(BaseHTTPRequestHandler):
    def _set_headers(self, code):
        self.send_response(code)
        self.send_header('Content-type', 'application/json')
        self.end_headers()

    def do_GET(self):
        error_msg = "unknown url or action"
        mpath, margs = urllib.splitquery(self.path)
        if mpath == "/services":
            action = ''
            obregion = ''
            margs_list = margs.split('&')
            for item in margs_list:
                if 'Action' in item:
                    action = item.split('=')[1]
                if 'ObRegion' in item:
                    obregion = item.split('=')[1]
            if action == 'GetObProxyConfig':
                obregion_url_list = []
                with open(CONF_FILE, 'r') as fr:
                    for line in fr:
                        line_dict = json.loads(line)
                        obregion_url_list.append("{{\"ObRegion\":\"{0}\",\"ObRootServiceInfoUrl\":"
                                                 "\"http://{1}:{2}/services?Action=ObRootServiceInfo"
                                                 "&ObRegion={0}\"}}".format(line_dict['ObRegion'], SERVER, PORT))
                content = "{{\"ObRootServiceInfoUrlList\":[{0}]," \
                          "\"ObProxyBinUrl\":\"http://{1}:{2}/client?Action=GetObProxy\"," \
                          "\"Version\":\"0de5e25653a1a3af01a1daa737b199f5\",\"ObProxyDatabaseInfo\":" \
                          "{{\"MetaDataBase\":\"http://{1}:{2}/services?Action=ObRootServiceInfo&ObRegion=obdv1\"," \
                          "\"DataBase\":\"obproxy\",\"User\":\"root@obproxy\",\"Password\":\"ZmsU*14zc\"}}}}".format(','.join(obregion_url_list), SERVER, PORT)
                self._set_headers(200)
                self.wfile.write(CODE_200_TEMP.format(content))
                return
            if action == 'ObRootServiceInfo':
                if obregion:
                    rsinfo = ''
                    with open(CONF_FILE, 'r') as fr:
                        for line in fr:
                            line_dict = json.loads(line)
                            if line_dict['ObRegion'] == obregion:
                                rsinfo = line.strip('\n')
                                break
                    if rsinfo:
                        self._set_headers(200)
                        self.wfile.write(CODE_200_TEMP.format(rsinfo))
                        return
                    else:
                        error_msg = "ObRegion [{0}] NOT found".format(obregion)
        self._set_headers(400)
        self.wfile.write(CODE_400_TEMP.format(error_msg))

    def do_HEAD(self):
        self._set_headers(200)

    def do_POST(self):
        global FILE_LOCK
        error_msg = "unknown url or action"
        mpath, margs = urllib.splitquery(self.path)
        if mpath == "/services":
            action = ''
            obregion = ''
            margs_list = margs.split('&')
            for item in margs_list:
                if 'Action' in item:
                    action = item.split('=')[1]
                if 'ObRegion' in item:
                    obregion = item.split('=')[1]
            if action == 'ObRootServiceInfo':
                try:
                    post_data = self.rfile.read(int(self.headers.getheader('Content-Length')))
                    post_data_dict = json.loads(post_data)
                except ValueError:
                    error_msg = "post data is NOT json format"
                    self._set_headers(400)
                    self.wfile.write(CODE_400_TEMP.format(error_msg))
                    return
                obregion_new = post_data_dict['ObRegion']
                obregion_id_new = post_data_dict['ObRegionId']
                if obregion == obregion_new and obregion_id_new:
                    line_list = []
                    is_exist = False
                    is_illegal = False
                    FILE_LOCK.acquire()
                    try:
                        with open(CONF_FILE, 'r') as fr:
                            for line in fr:
                                line_dict = json.loads(line)
                                if obregion_new != line_dict['ObRegion'] and obregion_id_new != line_dict['ObRegionId']:
                                    pass
                                elif obregion_new == line_dict['ObRegion'] and obregion_id_new == line_dict['ObRegionId']:
                                    line = post_data + '\n'
                                    is_exist = True
                                elif obregion_new == line_dict['ObRegion'] and obregion_id_new != line_dict['ObRegionId']:
                                    is_illegal = True
                                    error_msg = "ObRegion already exists but trying to update with different ObRegionId"
                                elif obregion_new != line_dict['ObRegion'] and obregion_id_new == line_dict['ObRegionId']:
                                    is_illegal = True
                                    error_msg = "ObRegionId already exists but trying to update with different ObRegion"
                                line_list.append(line)
                        if not is_exist and not is_illegal:
                            line_list.append(post_data + '\n')
                        if not is_illegal:
                            with open(CONF_FILE, 'w+') as fw:
                                for line in line_list:
                                    fw.write(line)
                            content = CODE_200_TEMP.format(post_data)
                            self._set_headers(200)
                            self.wfile.write(content)
                            return
                    finally:
                        FILE_LOCK.release()
                else:
                    error_msg = "ObRegion in post data NOT match ObRegion in URL"
        self._set_headers(400)
        self.wfile.write(CODE_400_TEMP.format(error_msg))

    def do_DELETE(self):
        global FILE_LOCK
        error_msg = "unknown url or action"
        mpath, margs = urllib.splitquery(self.path)
        if mpath == "/services":
            action = ''
            obregion = ''
            margs_list = margs.split('&')
            for item in margs_list:
                if 'Action' in item:
                    action = item.split('=')[1]
                if 'ObRegion' in item:
                    obregion = item.split('=')[1]
            if action == 'ObRootServiceInfo':
                if obregion:
                    line_list = []
                    is_exist = False
                    FILE_LOCK.acquire()
                    try:
                        with open(CONF_FILE, 'r') as fr:
                            for line in fr:
                                line_dict = json.loads(line)
                                if obregion == line_dict['ObRegion']:
                                    is_exist = True
                                else:
                                    line_list.append(line)
                        if is_exist:
                            with open(CONF_FILE, 'w+') as fw:
                                for line in line_list:
                                    fw.write(line)
                            content = CODE_200_TEMP.format("ObRegion [{0}] deleted".format(obregion))
                            self._set_headers(200)
                            self.wfile.write(content)
                            return
                        else:
                            error_msg = "ObRegion NOT exists"
                    finally:
                        FILE_LOCK.release()
                else:
                    error_msg = "ObRegion param is missing"
        self._set_headers(400)
        self.wfile.write(CODE_400_TEMP.format(error_msg))


def run(server_class=HTTPServer, handler_class=ConfigUrlServer, port=80):
    try:
        server_address = ('', port)
        httpd = server_class(server_address, handler_class)
        print 'starting ConfigUrlServer ...'
        httpd.serve_forever()
    except KeyboardInterrupt:
        print "ConfigUrlServer stopped"


if __name__ == "__main__":
    CONF_FILE = 'configurl_server.conf'
    if not os.path.exists(CONF_FILE):
        f = open(CONF_FILE, 'w')
        f.close()
    if len(argv) == 3:
        SERVER = argv[1]
        PORT = argv[2]
        run(port=int(PORT))
    else:
        print "Usage: python argv[0] <IP> <PORT>"
        print "  e.g. python argv[0] 10.10.10.10 1080"
