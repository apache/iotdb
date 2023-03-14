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


from iotdb.mlnode.process.task import ForecastingTrainingTask

support_task_types = {
    'forecast_training_task': ForecastingTrainingTask
}


def create_task(task_configs, model_configs, data_configs, task_trial_map):
    task_class = task_configs["task_class"]
    if task_class not in support_task_types:
        raise RuntimeError(f'Unknown task type: ({task_class}), which'
                           f' should be one of {support_task_types.keys()}')
    task_fn = support_task_types[task_class]
    task = task_fn(
        task_configs,
        model_configs,
        data_configs,
        task_trial_map
    )
    return task
