/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.it.env.cluster;

import org.apache.iotdb.it.framework.IoTDBTestLogger;
import org.apache.iotdb.tsfile.utils.Pair;

import org.apache.commons.lang3.SystemUtils;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.iotdb.consensus.ConsensusFactory.IOT_CONSENSUS;
import static org.apache.iotdb.consensus.ConsensusFactory.RATIS_CONSENSUS;
import static org.apache.iotdb.consensus.ConsensusFactory.SIMPLE_CONSENSUS;
import static org.apache.iotdb.db.utils.DateTimeUtils.convertLongToDate;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.CLUSTER_CONFIGURATIONS;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.DEFAULT_CONFIG_NODE_NUM;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.DEFAULT_DATA_NODE_NUM;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.DELIMITER;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.DIR_TIME_REPLACEMENT;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.HIGH_PERFORMANCE_MODE;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.HIGH_PERFORMANCE_MODE_CONFIG_NODE_NUM;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.HIGH_PERFORMANCE_MODE_DATA_NODE_NUM;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.IOT_CONSENSUS_STR;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.LIGHT_WEIGHT_STANDALONE_MODE;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.LIGHT_WEIGHT_STANDALONE_MODE_CONFIG_NODE_NUM;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.LIGHT_WEIGHT_STANDALONE_MODE_DATA_NODE_NUM;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.LOCK_FILE_PATH;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.RATIS_CONSENSUS_STR;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.SCALABLE_SINGLE_NODE_MODE;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.SCALABLE_SINGLE_NODE_MODE_CONFIG_NODE_NUM;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.SCALABLE_SINGLE_NODE_MODE_DATA_NODE_NUM;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.SIMPLE_CONSENSUS_STR;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.STRONG_CONSISTENCY_CLUSTER_MODE;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.STRONG_CONSISTENCY_CLUSTER_MODE_CONFIG_NODE_NUM;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.STRONG_CONSISTENCY_CLUSTER_MODE_DATA_NODE_NUM;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.USER_DIR;

public class EnvUtils {

  public static int[] searchAvailablePorts() {
    while (true) {
      int randomPortStart = 1000 + (int) (Math.random() * (1999 - 1000));
      randomPortStart = randomPortStart * 10 + 1;
      String lockFilePath = getLockFilePath(randomPortStart);
      File lockFile = new File(lockFilePath);
      try {
        // Lock the ports first to avoid to be occupied by other ForkedBooters during ports
        // available detecting
        if (!lockFile.createNewFile()) {
          continue;
        }
        List<Integer> requiredPorts =
            IntStream.rangeClosed(randomPortStart, randomPortStart + 9)
                .boxed()
                .collect(Collectors.toList());
        if (checkPortsAvailable(requiredPorts)) {
          return requiredPorts.stream().mapToInt(Integer::intValue).toArray();
        }
      } catch (IOException e) {
        // ignore
      }
      // Delete the lock file if the ports can't be used or some error happens
      if (lockFile.exists() && !lockFile.delete()) {
        IoTDBTestLogger.logger.error("Delete lockfile {} failed", lockFilePath);
      }
    }
  }

  private static boolean checkPortsAvailable(List<Integer> ports) {
    String cmd = getSearchAvailablePortCmd(ports);
    try {
      Process proc = Runtime.getRuntime().exec(cmd);
      return proc.waitFor() == 1;
    } catch (IOException e) {
      // ignore
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    return false;
  }

  private static String getSearchAvailablePortCmd(List<Integer> ports) {
    if (SystemUtils.IS_OS_WINDOWS) {
      return getWindowsSearchPortCmd(ports);
    }
    return getUnixSearchPortCmd(ports);
  }

  private static String getWindowsSearchPortCmd(List<Integer> ports) {
    String cmd = "netstat -aon -p tcp | findStr ";
    return cmd
        + ports.stream().map(v -> "/C:'127.0.0.1:" + v + "'").collect(Collectors.joining(" "));
  }

  private static String getUnixSearchPortCmd(List<Integer> ports) {
    String cmd = "lsof -iTCP -sTCP:LISTEN -P -n | awk '{print $9}' | grep -E ";
    return cmd + ports.stream().map(String::valueOf).collect(Collectors.joining("|")) + "\"";
  }

  private static Pair<Integer, Integer> getClusterNodesNum(int index) {
    String valueStr = System.getProperty(CLUSTER_CONFIGURATIONS);
    if (valueStr == null) {
      return null;
    }

    try {
      switch (getValueOfIndex(valueStr, index)) {
        case LIGHT_WEIGHT_STANDALONE_MODE:
          return new Pair<>(
              Integer.parseInt(System.getProperty(LIGHT_WEIGHT_STANDALONE_MODE_CONFIG_NODE_NUM)),
              Integer.parseInt(System.getProperty(LIGHT_WEIGHT_STANDALONE_MODE_DATA_NODE_NUM)));
        case SCALABLE_SINGLE_NODE_MODE:
          return new Pair<>(
              Integer.parseInt(System.getProperty(SCALABLE_SINGLE_NODE_MODE_CONFIG_NODE_NUM)),
              Integer.parseInt(System.getProperty(SCALABLE_SINGLE_NODE_MODE_DATA_NODE_NUM)));
        case HIGH_PERFORMANCE_MODE:
          return new Pair<>(
              Integer.parseInt(System.getProperty(HIGH_PERFORMANCE_MODE_CONFIG_NODE_NUM)),
              Integer.parseInt(System.getProperty(HIGH_PERFORMANCE_MODE_DATA_NODE_NUM)));
        case STRONG_CONSISTENCY_CLUSTER_MODE:
          return new Pair<>(
              Integer.parseInt(System.getProperty(STRONG_CONSISTENCY_CLUSTER_MODE_CONFIG_NODE_NUM)),
              Integer.parseInt(System.getProperty(STRONG_CONSISTENCY_CLUSTER_MODE_DATA_NODE_NUM)));
        default:
          // Print nothing to avoid polluting test outputs
          return null;
      }
    } catch (NumberFormatException ignore) {
      return null;
    }
  }

  public static String getLockFilePath(int port) {
    return LOCK_FILE_PATH + port;
  }

  public static Pair<Integer, Integer> getNodeNum() {
    Pair<Integer, Integer> nodesNum = getClusterNodesNum(0);
    if (nodesNum != null) {
      return nodesNum;
    }
    return new Pair<>(
        getIntFromSysVar(DEFAULT_CONFIG_NODE_NUM, 1, 0),
        getIntFromSysVar(DEFAULT_DATA_NODE_NUM, 3, 0));
  }

  public static Pair<Integer, Integer> getNodeNum(int index) {
    Pair<Integer, Integer> nodesNum = getClusterNodesNum(index);
    if (nodesNum != null) {
      return nodesNum;
    }
    return new Pair<>(
        getIntFromSysVar(DEFAULT_CONFIG_NODE_NUM, 1, index),
        getIntFromSysVar(DEFAULT_DATA_NODE_NUM, 3, index));
  }

  public static String getFilePathFromSysVar(String key, int index) {
    String valueStr = System.getProperty(key);
    if (valueStr == null) {
      return null;
    }
    return System.getProperty(USER_DIR) + getValueOfIndex(valueStr, index);
  }

  public static int getIntFromSysVar(String key, int defaultValue, int index) {
    String valueStr = System.getProperty(key);
    if (valueStr == null) {
      return defaultValue;
    }

    String value = getValueOfIndex(valueStr, index);
    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Invalid property value: " + value + " of key " + key);
    }
  }

  public static String getValueOfIndex(String valueStr, int index) {
    String[] values = valueStr.split(DELIMITER);
    return index <= values.length - 1 ? values[index] : values[values.length - 1];
  }

  public static String getTimeForLogDirectory(long startTime) {
    return convertLongToDate(startTime, "ms").replace(":", DIR_TIME_REPLACEMENT);
  }

  public static String fromConsensusFullNameToAbbr(String consensus) {
    switch (consensus) {
      case SIMPLE_CONSENSUS:
        return SIMPLE_CONSENSUS_STR;
      case RATIS_CONSENSUS:
        return RATIS_CONSENSUS_STR;
      case IOT_CONSENSUS:
        return IOT_CONSENSUS_STR;
      default:
        throw new IllegalArgumentException("Unknown consensus type: " + consensus);
    }
  }

  public static String fromConsensusAbbrToFullName(String consensus) {
    switch (consensus) {
      case SIMPLE_CONSENSUS_STR:
        return SIMPLE_CONSENSUS;
      case RATIS_CONSENSUS_STR:
        return RATIS_CONSENSUS;
      case IOT_CONSENSUS_STR:
        return IOT_CONSENSUS;
      default:
        throw new IllegalArgumentException("Unknown consensus type: " + consensus);
    }
  }

  private EnvUtils() {
    throw new IllegalStateException("Utility class");
  }
}
