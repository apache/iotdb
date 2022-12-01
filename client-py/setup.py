#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.
#

import setuptools
import io


try:
    with io.open("README.md", encoding="utf-8") as f:
        long_description = f.read()
except FileNotFoundError:
    long_description = ""


print(long_description)

setuptools.setup(
    name="apache-iotdb",  # Replace with your own username
    version="1.0.0",
    author=" Apache Software Foundation",
    author_email="dev@iotdb.apache.org",
    description="Apache IoTDB client API",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/apache/iotdb",
    packages=setuptools.find_packages(),
    install_requires=[
        "thrift>=0.13.0",
        "pandas>=1.0.0,<1.99.99",
        "numpy>=1.0.0",
        "testcontainers>=2.0.0",
        "sqlalchemy>=1.3.16, <1.4, !=1.3.21",
        "sqlalchemy-utils>=0.37.8, <0.38",
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Topic :: Software Development :: Libraries",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    python_requires=">=3.7",
    license="Apache License, Version 2.0",
    website="https://iotdb.apache.org",
    entry_points={
        "sqlalchemy.dialects": [
            "iotdb = iotdb.sqlalchemy.IoTDBDialect:IoTDBDialect",
        ],
    },
)
