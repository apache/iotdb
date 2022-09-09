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
package org.apache.iotdb.commons.enums;

import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.conf.CommonDescriptor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public enum HandleSystemErrorStrategy {
  /** set system status to read-only and the system only accepts query operations */
  CHANGE_TO_READ_ONLY,
  /** the system will be shutdown */
  SHUTDOWN;

  private static final Logger logger = LoggerFactory.getLogger(HandleSystemErrorStrategy.class);

  public void handle() {
    if (this == HandleSystemErrorStrategy.CHANGE_TO_READ_ONLY) {
      logger.error(
          "Unrecoverable error occurs! Change system status to read-only because handle_system_error is CHANGE_TO_READ_ONLY. Only query statements are permitted!",
          new RuntimeException("System mode is set to READ_ONLY"));
      CommonDescriptor.getInstance().getConfig().setNodeStatus(NodeStatus.ReadOnly);
    } else if (this == HandleSystemErrorStrategy.SHUTDOWN) {
      logger.error(
          "Unrecoverable error occurs! Shutdown system directly because handle_system_error is SHUTDOWN.",
          new RuntimeException("Unrecoverable error occurs! Shutdown system directly."));
      System.exit(-1);
    }
  }
}
