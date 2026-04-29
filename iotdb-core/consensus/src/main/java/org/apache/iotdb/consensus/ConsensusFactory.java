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

package org.apache.iotdb.consensus;

import org.apache.iotdb.commons.consensus.iotv2.container.IoTV2GlobalComponentContainer;
import org.apache.iotdb.consensus.config.ConsensusConfig;
import org.apache.iotdb.consensus.pipe.metric.IoTConsensusV2SyncLagManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Optional;

public class ConsensusFactory {
  public static final String CONSTRUCT_FAILED_MSG =
      "Construct consensusImpl failed, Please check your consensus className %s";

  public static final String SIMPLE_CONSENSUS = "org.apache.iotdb.consensus.simple.SimpleConsensus";
  public static final String RATIS_CONSENSUS = "org.apache.iotdb.consensus.ratis.RatisConsensus";
  public static final String IOT_CONSENSUS = "org.apache.iotdb.consensus.iot.IoTConsensus";
  // Keep the pre-rename class name for stale system properties / snapshots restored after a
  // jar-only upgrade.
  public static final String LEGACY_IOT_CONSENSUS_V2 =
      "org.apache.iotdb.consensus.pipe.PipeConsensus";
  public static final String REAL_IOT_CONSENSUS_V2 =
      "org.apache.iotdb.consensus.pipe.IoTConsensusV2";
  public static final String IOT_CONSENSUS_V2 = "org.apache.iotdb.consensus.iot.IoTConsensusV2";
  public static final String IOT_CONSENSUS_V2_BATCH_MODE = "batch";
  public static final String IOT_CONSENSUS_V2_STREAM_MODE = "stream";

  private static final Logger logger = LoggerFactory.getLogger(ConsensusFactory.class);

  private ConsensusFactory() {
    throw new IllegalStateException("Utility class ConsensusFactory");
  }

  // Downstream code compares against IOT_CONSENSUS_V2 directly, so persisted legacy names must be
  // normalized to the canonical constant before they fan out.
  public static String normalizeConsensusProtocolClass(String className) {
    if (className == null) {
      return null;
    }
    if (LEGACY_IOT_CONSENSUS_V2.equals(className) || REAL_IOT_CONSENSUS_V2.equals(className)) {
      return IOT_CONSENSUS_V2;
    }
    return className;
  }

  public static Optional<IConsensus> getConsensusImpl(
      String className, ConsensusConfig config, IStateMachine.Registry registry) {
    try {
      className = normalizeConsensusProtocolClass(className);
      // special judge for IoTConsensusV2
      if (IOT_CONSENSUS_V2.equals(className)) {
        className = REAL_IOT_CONSENSUS_V2;
        // initialize iotConsensusV2's thrift component
        IoTV2GlobalComponentContainer.build();
        // initialize iotConsensusV2's metric component
        IoTConsensusV2SyncLagManager.build();
      }
      Class<?> executor = Class.forName(className);
      Constructor<?> executorConstructor =
          executor.getDeclaredConstructor(ConsensusConfig.class, IStateMachine.Registry.class);
      executorConstructor.setAccessible(true);
      return Optional.of((IConsensus) executorConstructor.newInstance(config, registry));
    } catch (ClassNotFoundException
        | NoSuchMethodException
        | InstantiationException
        | IllegalAccessException
        | InvocationTargetException e) {
      logger.error("Couldn't Construct IConsensus class: {}", className, e);
    }
    return Optional.empty();
  }
}
