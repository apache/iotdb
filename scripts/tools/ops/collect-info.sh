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
echo Start collecting info
echo ---------------------

if [ -z "${IOTDB_HOME}" ]; then
    export IOTDB_HOME="`dirname "$0"`/../.."
  fi

COLLECTION_FILE="collection.txt"
COLLECTION_DIR="iotdb-info"

HELP="Usage: $0 [-h <ip>] [-p <port>] [-u <username>] [-pw <password>] [-jp <jdk_path>] [-dd <data_dir>]"

user_param="root"
passwd_param="root"
host_param="127.0.0.1"
port_param="6667"
jdk_path_param=""
data_dir_param="data/datanode/data"

get_property_value() {
    local file="$1"  # Properties path
    local key="$2"   # key name
    local value=""

    if [ -f "$file" ]; then
        local line=$(grep "^$key=" "$file")
        value="${line#*=}"
    fi

    echo "$value"
}

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

if [ -f "$IOTDB_HOME/conf/iotdb-system.properties" ]; then
  properties_file="$IOTDB_HOME/conf/iotdb-system.properties"
else
  properties_file="$IOTDB_HOME/conf/iotdb-datanode.properties"
fi
data_dir_key="dn_data_dirs"
value=$(get_property_value "$properties_file" "$data_dir_key")
if [ -n "$value" ]; then
  data_dir_param=$value
fi

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

rm -rf $IOTDB_HOME/$COLLECTION_DIR
mkdir -p $IOTDB_HOME/$COLLECTION_DIR/logs

{
    echo '===================== System Info ====================='
    case "$(uname)" in
        Linux)
             read -r system_memory unused_memory <<< "$(free | awk '/Mem:/{print $2, $4}')"
            system_cpu_cores=$(grep -c 'processor' /proc/cpuinfo)
            system_cpu_name=$(grep 'model name' /proc/cpuinfo | head -n 1 | awk -F ':' '{$1=$1}1')
            system_os_info=$(get_property_value /etc/os-release PRETTY_NAME)
            ;;
        FreeBSD)
            read -r system_memory_in_bytes unused_memory <<< "$(sysctl -n hw.realmem vm.stats.vm.v_inactive_count)"
            system_memory=$((system_memory_in_bytes / 1024))
            unused_memory=$(sysctl -n vm.stats.vm.v_inactive_count)
            system_cpu_cores=$(sysctl -n hw.ncpu)
            system_cpu_name=$(sysctl -n hw.model)
            system_os_info=$(uname -r)
            ;;
        SunOS)
            read -r system_memory unused_memory <<< "$(prtconf | awk '/Memory size:/ {print $3}') $(kstat -p unix:0:system_pages:pagestotal -s unix:0:system_pages:pagesfree | awk '{print $2}')"
            system_cpu_cores=$(psrinfo | wc -l)
            system_cpu_name=$(psrinfo -pv | grep "The" | awk -F':' '{print $2}' | awk '{$1=$1}1' | head -n 1)
            system_os_info=$(uname -v)
            ;;
        Darwin)
            read -r system_memory_in_bytes unused_memory <<< "$(sysctl -n hw.memsize) $(vm_stat | awk '/Pages free/ {print $3 * 4}')"
            system_memory=$((system_memory_in_bytes / 1024))
            system_cpu_cores=$(sysctl -n hw.ncpu)
            system_cpu_name=$(sysctl -n machdep.cpu.brand_string)
            system_os_info=$(sw_vers -productName)
            ;;
        *)
            system_memory="Unknown"
            unused_memory="Unknown"
            system_cpu_cores="Unknown"
            system_cpu_name="Unknown"
            system_os_info="Unknown"
            ;;
    esac
    echo "Operating System: $system_os_info"
    echo "CPU Name: $system_cpu_name"
    echo "CPU Cores: $system_cpu_cores"
    total_unit=$(choose_unit "$system_memory")
    echo "System Memory Total: $(convert_unit "$system_memory" "$total_unit") $total_unit"
    unuse_unit=$(choose_unit "$unused_memory")
    echo "Unused Memory: $(convert_unit "$unused_memory" "$unuse_unit") $unuse_unit"

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

{
  if [[ -d "$IOTDB_HOME/logs/" ]]; then
     for file in $IOTDB_HOME/logs/*.log; do
         if [[ $file =~ \.log$ ]]; then
             cp "$file" "$IOTDB_HOME/$COLLECTION_DIR/logs"
         fi
     done
   else
      echo "Directory $IOTDB_HOME/logs/ does not exist."
   fi
}

calculate_directory_size() {
    local file_type="$1"
    local total_size=0
    IFS=',' read -ra dirs <<< "$data_dir_param"
    for dir in "${dirs[@]}"; do
        if [[ $dir == /* ]]; then
            iotdb_data_dir="$dir/$file_type"
        else
            iotdb_data_dir="$IOTDB_HOME/$dir/$file_type"
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
    IFS=',' read -ra dirs <<< "$data_dir_param"
    for dir in "${dirs[@]}"; do
      if [[ $dir == /* ]]; then
        iotdb_data_dir="$dir/$file_type"
      else
        iotdb_data_dir="$IOTDB_HOME/$dir/$file_type"
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
          export JAVA_HOME="$jdk_path_param";"$IOTDB_HOME"/sbin/start-cli.sh -h "$host_param" -p "$port_param" -u "$user_param" -pw "$passwd_param" -e "$command"
        else
          "$IOTDB_HOME"/sbin/start-cli.sh -h "$host_param" -p "$port_param" -u "$user_param" -pw "$passwd_param" -e "$command"
        fi
    } >> "$COLLECTION_FILE"
}

execute_command_and_append_to_file 'show version'
execute_command_and_append_to_file 'show cluster details'
execute_command_and_append_to_file 'show regions'
execute_command_and_append_to_file 'show databases'
execute_command_and_append_to_file 'count devices'
execute_command_and_append_to_file 'count timeseries'


mv $COLLECTION_FILE $IOTDB_HOME/$COLLECTION_DIR
cp -r $IOTDB_HOME/conf $IOTDB_HOME/$COLLECTION_DIR
zip -r "$IOTDB_HOME/$zip_name" $IOTDB_HOME/$COLLECTION_DIR
echo "Program execution completed, file name is $zip_name"