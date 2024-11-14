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

import org.apache.commons.lang3.SystemUtils;
import org.apache.tsfile.utils.Pair;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.iotdb.consensus.ConsensusFactory.IOT_CONSENSUS;
import static org.apache.iotdb.consensus.ConsensusFactory.IOT_CONSENSUS_V2;
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
import static org.apache.iotdb.it.env.cluster.ClusterConstant.IOT_CONSENSUS_V2_STR;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.LIGHT_WEIGHT_STANDALONE_MODE;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.LIGHT_WEIGHT_STANDALONE_MODE_CONFIG_NODE_NUM;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.LIGHT_WEIGHT_STANDALONE_MODE_DATA_NODE_NUM;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.LOCK_FILE_PATH;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.PIPE_CONSENSUS_BATCH_MODE;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.PIPE_CONSENSUS_BATCH_MODE_CONFIG_NODE_NUM;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.PIPE_CONSENSUS_BATCH_MODE_DATA_NODE_NUM;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.PIPE_CONSENSUS_STREAM_MODE;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.PIPE_CONSENSUS_STREAM_MODE_CONFIG_NODE_NUM;
import static org.apache.iotdb.it.env.cluster.ClusterConstant.PIPE_CONSENSUS_STREAM_MODE_DATA_NODE_NUM;
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
    int length = 10;
    while (true) {
      int randomPortStart = 1000 + (int) (Math.random() * (1999 - 1000));
      randomPortStart = randomPortStart * (length + 1) + 1;
      final String lockFilePath = getLockFilePath(randomPortStart);
      final File lockFile = new File(lockFilePath);
      try {
        // Lock the ports first to avoid to be occupied by other ForkedBooters during ports
        // available detecting
        if (!lockFile.createNewFile()) {
          continue;
        }
        final List<Integer> requiredPorts =
            IntStream.rangeClosed(randomPortStart, randomPortStart + length)
                .boxed()
                .collect(Collectors.toList());
        if (checkPortsAvailable(requiredPorts)) {
          return requiredPorts.stream().mapToInt(Integer::intValue).toArray();
        }
      } catch (final IOException ignore) {
        // ignore
      }
      // Delete the lock file if the ports can't be used or some error happens
      if (lockFile.exists() && !lockFile.delete()) {
        IoTDBTestLogger.logger.error("Delete lockfile {} failed", lockFilePath);
      }
    }
  }

  private static boolean checkPortsAvailable(final List<Integer> ports) {
    final String cmd = getSearchAvailablePortCmd(ports);
    try {
      return Runtime.getRuntime().exec(cmd).waitFor() == 1;
    } catch (final IOException ignore) {
      // ignore
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    return false;
  }

  private static String getSearchAvailablePortCmd(final List<Integer> ports) {
    return SystemUtils.IS_OS_WINDOWS ? getWindowsSearchPortCmd(ports) : getUnixSearchPortCmd(ports);
  }

  private static String getWindowsSearchPortCmd(final List<Integer> ports) {
    return "netstat -aon -p tcp | findStr "
        + ports.stream().map(v -> "/C:'127.0.0.1:" + v + "'").collect(Collectors.joining(" "));
  }

  private static String getUnixSearchPortCmd(final List<Integer> ports) {
    return "lsof -iTCP -sTCP:LISTEN -P -n | awk '{print $9}' | grep -E "
        + ports.stream().map(String::valueOf).collect(Collectors.joining("|"))
        + "\"";
  }

  private static Pair<Integer, Integer> getClusterNodesNum(final int index) {
    final String valueStr = System.getProperty(CLUSTER_CONFIGURATIONS);
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
        case PIPE_CONSENSUS_BATCH_MODE:
          return new Pair<>(
              Integer.parseInt(System.getProperty(PIPE_CONSENSUS_BATCH_MODE_CONFIG_NODE_NUM)),
              Integer.parseInt(System.getProperty(PIPE_CONSENSUS_BATCH_MODE_DATA_NODE_NUM)));
        case PIPE_CONSENSUS_STREAM_MODE:
          return new Pair<>(
              Integer.parseInt(System.getProperty(PIPE_CONSENSUS_STREAM_MODE_CONFIG_NODE_NUM)),
              Integer.parseInt(System.getProperty(PIPE_CONSENSUS_STREAM_MODE_DATA_NODE_NUM)));
        default:
          // Print nothing to avoid polluting test outputs
          return null;
      }
    } catch (final NumberFormatException ignore) {
      return null;
    }
  }

  public static String getLockFilePath(final int port) {
    return LOCK_FILE_PATH + port;
  }

  public static Pair<Integer, Integer> getNodeNum() {
    final Pair<Integer, Integer> nodesNum = getClusterNodesNum(0);
    if (nodesNum != null) {
      return nodesNum;
    }
    return new Pair<>(
        getIntFromSysVar(DEFAULT_CONFIG_NODE_NUM, 1, 0),
        getIntFromSysVar(DEFAULT_DATA_NODE_NUM, 3, 0));
  }

  public static Pair<Integer, Integer> getNodeNum(final int index) {
    final Pair<Integer, Integer> nodesNum = getClusterNodesNum(index);
    if (nodesNum != null) {
      return nodesNum;
    }
    return new Pair<>(
        getIntFromSysVar(DEFAULT_CONFIG_NODE_NUM, 1, index),
        getIntFromSysVar(DEFAULT_DATA_NODE_NUM, 3, index));
  }

  public static String getFilePathFromSysVar(final String key, final int index) {
    final String valueStr = System.getProperty(key);
    if (valueStr == null) {
      return null;
    }
    return System.getProperty(USER_DIR) + getValueOfIndex(valueStr, index);
  }

  public static int getIntFromSysVar(final String key, final int defaultValue, final int index) {
    final String valueStr = System.getProperty(key);
    if (valueStr == null) {
      return defaultValue;
    }

    final String value = getValueOfIndex(valueStr, index);
    try {
      return Integer.parseInt(value);
    } catch (final NumberFormatException e) {
      throw new IllegalArgumentException("Invalid property value: " + value + " of key " + key);
    }
  }

  public static String getValueOfIndex(final String valueStr, final int index) {
    final String[] values = valueStr.split(DELIMITER);
    return index <= values.length - 1 ? values[index] : values[values.length - 1];
  }

  public static String getTimeForLogDirectory(final long startTime) {
    return convertLongToDate(startTime, "ms").replace(":", DIR_TIME_REPLACEMENT);
  }

  public static String fromConsensusFullNameToAbbr(final String consensus) {
    switch (consensus) {
      case SIMPLE_CONSENSUS:
        return SIMPLE_CONSENSUS_STR;
      case RATIS_CONSENSUS:
        return RATIS_CONSENSUS_STR;
      case IOT_CONSENSUS:
        return IOT_CONSENSUS_STR;
      case IOT_CONSENSUS_V2:
        return IOT_CONSENSUS_V2_STR;
      default:
        throw new IllegalArgumentException("Unknown consensus type: " + consensus);
    }
  }

  public static String fromConsensusAbbrToFullName(final String consensus) {
    switch (consensus) {
      case SIMPLE_CONSENSUS_STR:
        return SIMPLE_CONSENSUS;
      case RATIS_CONSENSUS_STR:
        return RATIS_CONSENSUS;
      case IOT_CONSENSUS_STR:
        return IOT_CONSENSUS;
      case IOT_CONSENSUS_V2_STR:
        return IOT_CONSENSUS_V2;
      default:
        throw new IllegalArgumentException("Unknown consensus type: " + consensus);
    }
  }

  private EnvUtils() {
    throw new IllegalStateException("Utility class");
  }
}
