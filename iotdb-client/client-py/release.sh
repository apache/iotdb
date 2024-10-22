#!/bin/bash
#
#
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

# the python version must be python3.
python3 --version

rm -Rf build
rm -Rf dist
rm -Rf iotdb_session.egg_info
rm -f pyproject.toml

# (Re-)build generated code
(cd ../..; mvn clean package -pl iotdb-client/client-py -am)

# Run unit tests
if [ "$1" == "test" ]; then
  pytest .
fi

# See https://packaging.python.org/tutorials/packaging-projects/
#python3 setup.py sdist bdist_wheel
python3 -m build
if [ "$1" == "release" ]; then
  python3 -m twine upload  dist/*
fi
