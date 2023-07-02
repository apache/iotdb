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
from abc import abstractmethod


class Validator(object):
    @abstractmethod
    def validate(self, value):
        """
        Checks whether the given value is valid.

        Parameters:
        - value: The value to validate

        Returns:
        - True if the value is valid, False otherwise.
        """
        raise NotImplementedError("Subclasses must implement the validate() method.")


class FloatRangeValidator(Validator):
    def __init__(self, min_value, max_value):
        self.min_value = min_value
        self.max_value = max_value

    def validate(self, value):
        if isinstance(value, float) and self.min_value <= value <= self.max_value:
            return True
        return False
