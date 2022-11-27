#!/bin/sh
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


# this function is for parsing the variables like "A=B" in  `start-server.sh -D A=B`
# The command just parse IOTDB-prefixed variables and ignore all other variables
checkEnvVariables()
{

  string="$1"
  array=$(echo $string | tr '=' ' ')
  eval set -- "$array"
  case "$1" in
          IOTDB_INCLUDE)
               IOTDB_INCLUDE="$2"
          ;;
          IOTDB_HOME)
               IOTDB_HOME="$2"
          ;;
          IOTDB_DATA_HOME)
             IOTDB_DATA_HOME="$2"
          ;;
          IOTDB_CONF)
             IOTDB_CONF="$2"
          ;;
          IOTDB_LOG_DIR)
             IOTDB_LOG_DIR="$2"
          ;;
          IOTDB_LOG_CONFIG)
             IOTDB_LOG_CONFIG="$2"
          ;;
          IOTDB_CLI_CONF)
             IOTDB_CLI_CONF="$2"
          ;;
          *)
            #we can not process it, so that we return back.
            echo "$1=$2"
          ;;
  esac
  echo ""
}

checkConfigNodeEnvVariables()
{

  string="$1"
  array=$(echo $string | tr '=' ' ')
  eval set -- "$array"
  case "$1" in
          IOTDB_INCLUDE)
            IOTDB_INCLUDE="$2"
          ;;
          CONFIGNODE_HOME)
              CONFIGNODE_HOME="$2"
          ;;
          CONFIGNODE_DATA_HOME)
             CONFIGNODE_DATA_HOME="$2"
          ;;
          I_CONF)
             IOTDB_CONF="$2"
          ;;
          CONFIGNODE_LOG_DIR)
             CONFIGNODE_LOG_DIR="$2"
          ;;
          CONFIGNODE_LOG_CONFIG)
             CONFIGNODE_LOG_CONFIG="$2"
          ;;
          *)
            #we can not process it, so that we return back.
            echo "$1=$2"
          ;;
  esac
  echo ""
}

checkAllVariables()
{
  if [ -z "${IOTDB_INCLUDE}" ]; then
    #do nothing
    :
  elif [ -r "$IOTDB_INCLUDE" ]; then
      . "$IOTDB_INCLUDE"
  fi

  if [ -z "${IOTDB_HOME}" ]; then
    export IOTDB_HOME="`dirname "$0"`/.."
  fi

  if [ -z "${IOTDB_DATA_HOME}" ]; then
    export IOTDB_DATA_HOME=${IOTDB_HOME}
  fi

  if [ -z "${IOTDB_CONF}" ]; then
    export IOTDB_CONF=${IOTDB_HOME}/conf
  fi

  if [ -z "${IOTDB_LOG_DIR}" ]; then
    export IOTDB_LOG_DIR=${IOTDB_HOME}/logs
  fi

  if [ -z "${IOTDB_LOG_CONFIG}" ]; then
    export IOTDB_LOG_CONFIG="${IOTDB_CONF}/logback-datanode.xml"
  fi
}

checkAllConfigNodeVariables()
{
  if [ -z "${IOTDB_INCLUDE}" ]; then
    #do nothing
    :
  elif [ -r "$IOTDB_INCLUDE" ]; then
      . "$IOTDB_INCLUDE"
  fi

  if [ -z "${CONFIGNODE_HOME}" ]; then
    export CONFIGNODE_HOME="`dirname "$0"`/.."
  fi

  if [ -z "${CONFIGNODE_DATA_HOME}" ]; then
    export CONFIGNODE_DATA_HOME=${CONFIGNODE_HOME}
  fi

  if [ -z "${CONFIGNODE_CONF}" ]; then
    export CONFIGNODE_CONF=${CONFIGNODE_HOME}/conf
  fi

  if [ -z "${CONFIGNODE_LOG_DIR}" ]; then
    export CONFIGNODE_LOG_DIR=${CONFIGNODE_HOME}/logs
  fi

  if [ -z "${CONFIGNODE_LOG_CONFIG}" ]; then
    export CONFIGNODE_LOG_CONFIG="${CONFIGNODE_CONF}/logback-confignode.xml"
  fi
}

initEnv() {
  if [ -f "$IOTDB_CONF/datanode-env.sh" ]; then
      if [ "x$PRINT_GC" != "x" ]; then
        . "$IOTDB_CONF/datanode-env.sh" "printgc"
      else
          . "$IOTDB_CONF/datanode-env.sh"
      fi
  elif [ -f "${IOTDB_HOME}/conf/datanode-env.sh" ]; then
      if [ "x$PRINT_GC" != "x" ]; then
        . "${IOTDB_HOME}/conf/datanode-env.sh" "printgc"
      else
        . "${IOTDB_HOME}/conf/datanode-env.sh"
      fi
  else
      echo "Can't find $IOTDB_CONF/datanode-env.sh"
  fi
}

initConfigNodeEnv() {
  if [ -f "$CONFIGNODE_CONF/confignode-env.sh" ]; then
      if [ "x$PRINT_GC" != "x" ]; then
        . "$CONFIGNODE_CONF/confignode-env.sh" "printgc"
      else
          . "$CONFIGNODE_CONF/confignode-env.sh"
      fi
  elif [ -f "${CONFIGNODE_HOME}/conf/confignode-env.sh" ]; then
      if [ "x$PRINT_GC" != "x" ]; then
        . "${CONFIGNODE_HOME}/conf/confignode-env.sh" "printgc"
      else
        . "${CONFIGNODE_HOME}/conf/confignode-env.sh"
      fi
  else
      echo "can't find $CONFIGNODE_CONF/confignode-env.sh"
  fi
}

get_iotdb_include() {
  #reset $1 to $* for this command
  eval set -- "$1"
  VARS=""
  while true; do
      case "$1" in
          -D)
              VARS="$VARS $(checkEnvVariables $2)"
              shift 2
          ;;
          "")
            #if we do not use getopt, we then have to process the case that there is no argument.
            #in some systems, when there is no argument, shift command may throw error, so we skip directly
              break
          ;;
          *)
              VARS="$VARS $1"
              shift
          ;;
      esac
  done
  echo "$VARS"
}