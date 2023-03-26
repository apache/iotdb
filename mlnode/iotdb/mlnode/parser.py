import argparse


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


class ConfigParser(argparse.ArgumentParser):
    def __init__(self, *args, **kwargs):
        super(ConfigParser, self).__init__(*args, **kwargs)

    def parse_configs(self, config):
        conf_dict = vars(self.parse_args([]))
        args = []
        for k, v in config.items():
            if k in conf_dict.keys():
                args.append("--{}".format(k))
                args.append(eval(v) if type(conf_dict[k]) is list and type(v) is not list else v)
        return vars(self.parse_args(args))
