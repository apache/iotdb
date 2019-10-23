#!/usr/bin/env bash

################################################################################
##
##  Licensed to the Apache Software Foundation (ASF) under one or more
##  contributor license agreements.  See the NOTICE file distributed with
##  this work for additional information regarding copyright ownership.
##  The ASF licenses this file to You under the Apache License, Version 2.0
##  (the "License"); you may not use this file except in compliance with
##  the License.  You may obtain a copy of the License at
##
##      http://www.apache.org/licenses/LICENSE-2.0
##
##  Unless required by applicable law or agreed to in writing, software
##  distributed under the License is distributed on an "AS IS" BASIS,
##  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
##  See the License for the specific language governing permissions and
##  limitations under the License.
##
################################################################################

# Remove all the css and js directories except the ones in the root of the site.
find target/staging -type d | grep 'target\/staging\/.*\/css$' | xargs rm -r
find target/staging -type d | grep 'target\/staging\/.*\/js$' | xargs rm -r

# Delete some individual images.
find target/staging -type f | grep 'target\/staging\/.*/images/close\.png$' | xargs rm
find target/staging -type f | grep 'target\/staging\/.*/images/close\.gif$' | xargs rm
find target/staging -type f | grep 'target\/staging\/.*/images/loading\.png$' | xargs rm
find target/staging -type f | grep 'target\/staging\/.*/images/loading\.gif$' | xargs rm
find target/staging -type f | grep 'target\/staging\/.*/images/next\.png$' | xargs rm
find target/staging -type f | grep 'target\/staging\/.*/images/prev\.png$' | xargs rm

# Delete any now empty directories.
find target/staging -type d -empty | xargs rm -r