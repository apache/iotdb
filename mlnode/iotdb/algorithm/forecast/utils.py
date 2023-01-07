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


import argparse


def parseModelConfig(modelConfig):
    modelType = modelConfig.model_type
    if modelType == 'DLinear':
        config = argparse.Namespace(
            model_type = modelConfig.model_type,
            seq_len=int(modelConfig.input_length),
            pred_len=int(modelConfig.output_length),
            enc_in=int(modelConfig.num_series),
            individual=modelConfig.individual == str(True),
        )
        # default config
        config.kernel_size = 25
    else:
        raise NotImplementedError

    return config