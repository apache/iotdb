#!/bin/bash
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

source_file="src/assembly/resources/conf/iotdb-system.properties"
target_template_file="target/conf/iotdb-system.properties.template"
target_properties_file="target/conf/iotdb-system.properties"

if [ -f "$target_template_file" ]; then
    rm "$target_template_file"
fi

if [ -f "$target_properties_file" ]; then
    rm "$target_properties_file"
fi

mkdir -p "$(dirname "$target_template_file")"

cp "$source_file" "$target_template_file"

# Add license header to the target properties file
cat <<EOL > "$target_properties_file"
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

EOL

grep -v '^\s*#' "$target_template_file" | grep -v '^\s*$' >> "$target_properties_file"
