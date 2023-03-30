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


import re
import sys
import argparse
from iotdb.mlnode.exception import MissingConfigError, WrongTypeError


class ConfigParser(argparse.ArgumentParser):
    def __init__(self):
        super().__init__()

    def parse_configs(self, configs):
        args = self.parse_dict(configs)
        return vars(self.parse_known_args(args)[0])

    @staticmethod
    def parse_dict(config_dict):
        args = []
        for k, v in config_dict.items():
            args.append("--{}".format(k))
            if isinstance(v, str) and re.match(r'^\[(.*)\]$', v):
                v = eval(v)
                v = [str(i) for i in v]
                args.extend(v)
            elif isinstance(v, list):
                args.extend([str(i) for i in v])
            else:
                args.append(v)
        return args

    def error(self, message: str):
        # required arguments are missing
        if message.startswith('the following arguments are required:'):
            missing_arg = re.findall(r': --(\w+)', message)[0]
            raise MissingConfigError(missing_arg)
        elif re.match(r'argument --\w+: invalid \w+ value:', message):
            argument = re.findall(r'argument --(\w+):', message)[0]
            expected_type = re.findall(r'invalid (\w+) value:', message)[0]
            raise WrongTypeError(argument, expected_type)
        else:
            raise Exception(message)

        sys.exit()
