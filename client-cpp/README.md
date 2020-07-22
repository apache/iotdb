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

#### Mac

Make sure `Homebrew` ist installed in order to update `Bison` to a newer version (the version 2.3 installed per default is too old)

```
    /usr/bin/ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"
```

Then update `Bison`:

```
    brew install bison
    brew link bison --force
    echo 'export PATH="/usr/local/opt/bison/bin:$PATH"' >> ~/.bash_profile
```

#### Windows

Some tools need to be installed before being able to build on Windows:

* WinBuilds (for `with-cpp`, `with-proxies` profiles)
* Bison (for `with-cpp` profiles)
* Flex (for `with-cpp` profiles)
* Python 2.7 (for `with-python`, `with-proxies` profiles)
* WinPCAP
* OpenSSL