import sys
import traceback
from base64 import b64encode
from string import Template

import json
import requests

BUFFER_SIZE_BYTES = 2 ** 20  # 1MB offset


class LoadFile(object):

    def __init__(self, workspace_url, token):

        if not workspace_url.startswith("https") or not workspace_url.endswith(".net"):
            print("please enter correct url, exmaple: https://adb-8583429878437803.3.azuredatabricks.net")
            print("no / or space in the end")
            sys.exit()

        request_header_str = Template(
            '''{"Content-Type": "application/x-www-form-urlencoded", 
            "Authorization": "Bearer ${token}"}''').safe_substitute(
            token=token)

        self.workspace_url = workspace_url
        self.headers_json = json.loads(request_header_str)

    def add_block(self, text, handle):
        data_temp = '{ "data": "${base64Str}", "handle": ${handle} }'
        request_url = "{}/api/2.0/dbfs/add-block".format(self.workspace_url)
        data = Template(data_temp).safe_substitute(base64Str=b64encode(text).decode(),
                                                   handle=handle)
        try:
            r = requests.post(request_url,
                              headers=self.headers_json, data=data)
            r.raise_for_status()
            return r
        except requests.exceptions.HTTPError as err:
            raise SystemExit(err)

    def create_file(self, dbfs_path):
        request_url = "{}/api/2.0/dbfs/create".format(self.workspace_url)
        data = Template('{ "path": "${path}", "overwrite": true }').safe_substitute(path=dbfs_path)

        try:
            r = requests.post(request_url, headers=self.headers_json, data=data)
            r.raise_for_status()
            return r
        except requests.exceptions.HTTPError as err:
            raise SystemExit(err)

    def close(self, handle):
        request_url = "{}/api/2.0/dbfs/close".format(self.workspace_url)
        data = Template('{ "handle": ${handle}}').safe_substitute(handle=handle)
        try:
            r = requests.post(request_url, headers=self.headers_json, data=data)
            r.raise_for_status()
            return r
        except requests.exceptions.HTTPError as err:
            raise SystemExit(err)

    def upload_file(self, source_path, dbfs_path):
        handle = None
        try:
            r = self.create_file(dbfs_path)
            status_code = r.status_code

            handle = r.json()["handle"]
            with open(source_path, 'rb') as local_file:
                while True:
                    contents = local_file.read(BUFFER_SIZE_BYTES)
                    if len(contents) == 0:
                        break
                    r = self.add_block(contents, handle)
            r = self.close(handle)
            handle = None
            print("file upload is ok")
        except:
            traceback.print_exc()
        finally:
            if handle is not None:
                self.close(handle)


if __name__ == '__main__':
    #do some test
    test = LoadFile("https://adb-8583429878437803.3.azuredatabricks.net",
                    "<PAT token>")
    test.upload_file("C:/Users/leisun/Downloads/stdout--2022-03-01--01-00.txt", "/tmp/test.txt")
