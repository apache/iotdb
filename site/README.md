<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

# IoTDB website

[![Build Status](https://builds.apache.org/view/I/view/IoTDB/job/IoTDB%20Website/badge/icon)](https://builds.apache.org/view/I/view/IoTDB/job/IoTDB%20Website/)

See https://iotdb.apache.org/

## Build Setup

run `mvn site -DskipTests` for:

- get docs from the master branch and all lagecy docs remotely.
- download node.js and npm;
- run `npm install` and `npm run build`


## How to Debug

after running `mvn site -DskipTests`, all source codes are copied into target/vue-source

then if you want to debug, just run 
```
# serve with hot reload at localhost:8080
npm run dev

# build for production with minification
npm run build
```

Remeber, only the changes of site/src can be logged by Git. 
All changes in the target folder will be ignored by Git.

## Deploy Manually

run `mvn package scm-publish:publish-scm`.
 
Apache ID and passwored is needed.

## Directory Structure

```
.
├─ src/main
│  ├─ README.md	       //Home
│  └─ .vuepress
│  │  └─ components    //Global vue template
│  │  └─ public        //Store static files
│  │  └─ config.js	   //Configuration
│  └─ document         //document 
│  └─ download         //download
│  └─ ...              
└─ package.json
```

## FAQ
If you get an error on your MacOS:

> gyp: No Xcode or CLT version detected! 

Then, install CommandLine of Xcode: `sudo xcode-select --install`.
If you have installed and the error still occurs, then `sudo xcode-select --reset`

