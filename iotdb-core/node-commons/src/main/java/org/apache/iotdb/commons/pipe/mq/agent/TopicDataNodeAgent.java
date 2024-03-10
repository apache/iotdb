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

package org.apache.iotdb.commons.pipe.mq.agent;

import org.apache.iotdb.commons.pipe.mq.meta.PipeMQTopicMetaKeeper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class TopicDataNodeAgent {

  private static final Logger LOGGER = LoggerFactory.getLogger(TopicDataNodeAgent.class);

  private final PipeMQTopicMetaKeeper pipeMQTopicMetaKeeper;

  public TopicDataNodeAgent() {
    pipeMQTopicMetaKeeper = new PipeMQTopicMetaKeeper();
  }

  ////////////////////////// PipeMQTopicMeta Lock Control //////////////////////////
  //  private void acquireReadLock() {
  //    pipeMQTopicMetaKeeper.acquireReadLock();
  //  }
  //
  //  private void releaseReadLock() {
  //    pipeMQTopicMetaKeeper.releaseReadLock();
  //  }
  //
  //  private void acquireWriteLock() {
  //    pipeMQTopicMetaKeeper.acquireWriteLock();
  //  }
  //
  //  private void releaseWriteLock() {
  //    pipeMQTopicMetaKeeper.releaseWriteLock();
  //  }

  ////////////////////////// Manage by Topic Name //////////////////////////

  //  private boolean createPipeMQTopic(PipeMQTopicMeta topicMetaFromCoordinator) {
  //    final String topicName = topicMetaFromCoordinator.getTopicName();
  //
  //    final PipeMQTopicMeta existedPipeMQTopicMeta =
  //        pipeMQTopicMetaKeeper.getPipeMQTopicMeta(topicName);
  //    if (existedPipeMQTopicMeta != null) {
  //      if (!checkBeforeCreatePipeMQTopic(existedPipeMQTopicMeta, topicName)) {
  //        return false;
  //      }
  //      // todo: whether to drop the existed pipeMQTopicMeta?
  //    }
  //    return true;
  //  }

  private void dropPipeMQTopic(String topicName) {}

  ////////////////////////// Validate //////////////////////////

  public void validate(String topicName, Map<String, String> topicAttributes) throws Exception {}
}
