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

### Install npm and node in your computer

npm is just like maven, a package manager for the JavaScript. node is similar to javaSE, JavaScript runtime environment that executes JavaScript code outside of a browser.

#### MacOS

you can use
```
brew install node
brew install npm
```
to install them or use MacOS installer, download in https://nodejs.org/en/download/.

#### Windows

You can use the Windows Installer, which can be downloaded in https://nodejs.org/en/download/.

#### Ubuntu

```
sudo apt-get install nodejs
sudo apt-get install npm
```

#### CentOS

```
sudo yum install nodejs
sudo yum install npm
```

After install, you can check their versions to make sure they have been installed successfully.

```
npm -v
node -v
```

### Pack web resource

This is like maven packaging jars. 

Run `npm install` in the web package to install necessary dependencies, and then run `npm run-script build` to pack web resources.

you can find a bundle.js and a png in dist.

### Set IOTDB_HOME 

In order to access web static resource, you have to set IOTDB_HOME. For example, run

```
export IOTDB_HOME=${your_path}
```


