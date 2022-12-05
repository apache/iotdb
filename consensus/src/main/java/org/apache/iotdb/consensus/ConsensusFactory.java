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

import org.apache.iotdb.consensus.config.ConsensusConfig;

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

  private static final Logger logger = LoggerFactory.getLogger(ConsensusFactory.class);

  public static Optional<IConsensus> getConsensusImpl(
      String className, ConsensusConfig config, IStateMachine.Registry registry) {
    try {
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
