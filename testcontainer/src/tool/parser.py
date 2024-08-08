# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

import os
import sys
import re

pattern = re.compile(r"docker\-java\-stream\-+(\d+)")


def getAllLogs(filename):
    with open(filename, "r") as f:
        data = f.read()
        return data.split("up -d")[1:]


def writeAllLogs(filename, content):
    with open(filename, "w") as f:
        for row in content:
            f.write(row)
            f.write("\n")


def getNodes(log):
    ids = pattern.findall(log)
    nodes = {}
    for id in ids:
        if not nodes.__contains__(id):
            nodes[id] = []
    return nodes


def checkAndMkdir(i):
    if not os.path.exists("./{}".format(i)):
        os.mkdir("./{}".format(i))


def parse(nodes, rows):
    for row in rows:
        for id, content in nodes.items():
            if row.__contains__(id):
                content.append(row)
        if row.__contains__("[ERROR]"):
            for content in nodes.values():
                content.append(row)


def output(nodes, i):
    for key, content in nodes.items():
        writeAllLogs("./{}/{}_{}.txt".format(i, i, key), content)


if __name__ == "__main__":
    logs = getAllLogs(sys.argv[1])
    count = 0
    for i in range(len(logs)):
        if logs[i].__contains__("FAILURE!"):
            nodes = getNodes(logs[i])
            parse(nodes, logs[i].split("\n"))
            checkAndMkdir(i)
            output(nodes, i)
            count = count + 1

    print("find {} failed tests".format(count))
