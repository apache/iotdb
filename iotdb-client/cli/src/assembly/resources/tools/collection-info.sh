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

#!/bin/bash

echo ---------------------
echo Start Collection info
echo ---------------------

if [ -z "${IOTDB_HOME}" ]; then
    export IOTDB_HOME="`dirname "$0"`/.."
  fi

COLLECTION_FILE="collection.txt"

HELP="Usage: $0 [-h <ip>] [-p <port>] [-u <username>] [-pw <password>] [-jp <jdk_path>] [-dd <data_dir>]"

user_param="root"
passwd_param="root"
host_param="127.0.0.1"
port_param="6667"
jdk_path_param=""
data_dir_param="$IOTDB_HOME/data"

while true; do
    case "$1" in
        -u)
            user_param="$2"
            shift 2
            ;;
        -pw)
            passwd_param="$2"
            shift 2
        ;;
        -h)
            host_param="$2"
            shift 2
        ;;
        -p)
            port_param="$2"
            shift 2
        ;;
        -jp)
              jdk_path_param="$2"
              shift 2
          ;;
        -dd)
              data_dir_param="$2"
              shift 2
          ;;
        "")
              break
              ;;
        *)
            echo "Unrecognized options:$1"
            echo "${HELP}"
            exit 1
        ;;
    esac
done

timestamp=$(date +"%Y%m%d%H%M%S")

zip_name="collection-$timestamp.zip"

zip_directory="$IOTDB_HOME/"

files_to_zip="${COLLECTION_FILE} $IOTDB_HOME/conf"

{
    echo '====================== CPU Info ======================'
    cat /proc/cpuinfo | awk -F ':' '/model name/ {print $2}' | uniq | tr -d '\n'
    if [ -f /etc/centos-release ]; then
        echo -n ' '
        grep -c '^processor' /proc/cpuinfo | tr -d '\n'
        echo ' core'
    elif [ -f /etc/lsb-release ]; then
        echo -n ' '
        nproc | tr -d '\n'
        echo ' core'
    fi
} >> "$COLLECTION_FILE"

{
    echo '===================== Memory Info====================='
    free -h
} >> "$COLLECTION_FILE"

{
    echo '===================== System Info====================='
    if [ -f /etc/centos-release ]; then
        cat /etc/centos-release
    elif [ -f /etc/lsb-release ]; then
        awk -F '=' '/DESCRIPTION/ {print $2}' /etc/lsb-release | tr -d '"'
    else
        echo "Unsupported Linux distribution"
    fi
} >> "$COLLECTION_FILE"

{
    echo '===================== JDK Version====================='
    if [ -n "$jdk_path_param" ]; then
        if [ -d "$jdk_path_param" ]; then
            $jdk_path_param/bin/java -version 2>&1
        else
            echo "Invalid JDK path: $jdk_path_param"
            exit 1
        fi
    else
      if [ -n "$JAVA_HOME" ]; then
          for java in "$JAVA_HOME"/bin/amd64/java "$JAVA_HOME"/bin/java; do
              if [ -x "$java" ]; then
                  JAVA="$java"
                  break
              fi
          done
      else
          JAVA=java
      fi

      if [ -z $JAVA ] ; then
          echo Unable to find java executable. Check JAVA_HOME and PATH environment variables.  > /dev/stderr
          exit 1;
      fi
      java -version 2>&1
    fi
} >> "$COLLECTION_FILE"

{
    echo '=================== Activation Info===================='
    if [ -d "$(dirname "$0")/$IOTDB_HOME/activation" ]; then
        if [ -f "$(dirname "$0")/$IOTDB_HOME/activation/license" ]; then
            echo "Active"
        else
            echo "Not active"
        fi
    else
        echo "Open source version"
    fi
} >> "$COLLECTION_FILE"

convert_unit() {
    local size=$1
    local target_unit=$2
    local converted_size=$size

    if [ "$target_unit" == "M" ]; then
        converted_size=$(awk "BEGIN {printf \"%.2f\", $size / 1024}")
    elif [ "$target_unit" == "G" ]; then
        converted_size=$(awk "BEGIN {printf \"%.2f\", $size / (1024 * 1024)}")
    elif [ "$target_unit" == "T" ]; then
        converted_size=$(awk "BEGIN {printf \"%.2f\", $size / (1024 * 1024 * 1024)}")
    fi

    echo "$converted_size"
}

choose_unit() {
    local size=$1
    local unit=""

    if [ "$size" -lt 1024 ]; then
        unit="K"
    elif [ "$size" -lt 1048576 ]; then
        unit="M"
    elif [ "$size" -lt 1073741824 ]; then
        unit="G"
    else
        unit="T"
    fi

    echo "$unit"
}

calculate_directory_size() {
    local file_type="$1"
    local total_size=0
    IFS=' ' read -ra dirs <<< "$data_dir_param"
    for dir in "${dirs[@]}"; do
        iotdb_data_dir="$dir/datanode/data/$file_type"
        if [ -n "$data_dir_param" ]; then
              iotdb_data_dir="$dir/$file_type"
        else
              iotdb_data_dir="$dir/datanode/data/$file_type"
        fi
        if [ -d "$iotdb_data_dir" ]; then
            local size=$(du -s "$iotdb_data_dir" | awk '{print $1}')
            total_size=$((total_size + size))
        fi
    done
    echo "$total_size"
}

calculate_file_num() {
    local file_type="$1"
    local total_num=0
    IFS=' ' read -ra dirs <<< "$data_dir_param"
    for dir in "${dirs[@]}"; do
        if [ -n "$data_dir_param" ]; then
           iotdb_data_dir="$dir/$file_type"
        else
           iotdb_data_dir="$dir/datanode/data/$file_type"
        fi
        if [ -d "$iotdb_data_dir" ]; then
            local num=$(find "$iotdb_data_dir" -type f ! -name "*.tsfile.resource" | wc -l)
            total_num=$((total_num + num))
        fi
    done
    echo "$total_num"
}

{
    echo '===================== TsFile Info====================='
    sequence="sequence"
    unsequence="unsequence"
    echo "sequence(tsfile number): $(calculate_file_num "$sequence")"
    echo "unsequence(tsfile number): $(calculate_file_num "$unsequence")"
    total_size=$(calculate_directory_size "$sequence")
    unit=$(choose_unit "$total_size")
    total_size_in_unit=$(convert_unit "$total_size" "$unit")
    echo "sequence(tsfile size): $total_size_in_unit $unit"
    total_size=$(calculate_directory_size "$unsequence")
    unit=$(choose_unit "$total_size")
    total_size_in_unit=$(convert_unit "$total_size" "$unit")
    echo "unsequence(tsfile size): $total_size_in_unit $unit"
} >> "$COLLECTION_FILE"

execute_command_and_append_to_file() {
    local command=$1
    {
        echo "=================== $command ===================="
        if [ -n "$jdk_path_param" ]; then
          export JAVA_HOME="$jdk_path_param";"$IOTDB_HOME"/sbin/start-cli.sh -h "$host_param" -p "$port_param" -u "$user_param" -pw "$passwd_param" -e "$command" | sed '$d' | sed '$d'
        else
          "$IOTDB_HOME"/sbin/start-cli.sh -h "$host_param" -p "$port_param" -u "$user_param" -pw "$passwd_param" -e "$command" | sed '$d' | sed '$d'
        fi
    } >> "$COLLECTION_FILE"
}

execute_command_and_append_to_file 'show version'
execute_command_and_append_to_file 'show cluster details'
execute_command_and_append_to_file 'show regions'
execute_command_and_append_to_file 'show databases'
execute_command_and_append_to_file 'count devices'
execute_command_and_append_to_file 'count timeseries'

rm -rf $IOTDB_HOME/collectioninfo
mkdir -p $IOTDB_HOME/collectioninfo
mv $COLLECTION_FILE $IOTDB_HOME/collectioninfo
cp -r $IOTDB_HOME/conf $IOTDB_HOME/collectioninfo
zip -r "$IOTDB_HOME/$zip_name" $IOTDB_HOME/collectioninfo
echo "Program execution completed, file name is $zip_name"