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
import org.apache.iotdb.commons.i18n.CommonMessages;

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
          CommonMessages
              .LOG_UNRECOVERABLE_ERROR_OCCURS_CHANGE_SYSTEM_STATUS_READ_ONLY_BECAUSE_HANDLE_05C9AD1A,
          new RuntimeException(CommonMessages.SYSTEM_READ_ONLY));
      CommonDescriptor.getInstance().getConfig().setNodeStatus(NodeStatus.ReadOnly);
    } else if (this == HandleSystemErrorStrategy.SHUTDOWN) {
      logger.error(
          CommonMessages
              .LOG_UNRECOVERABLE_ERROR_OCCURS_SHUTDOWN_SYSTEM_DIRECTLY_BECAUSE_HANDLE_SYSTEM_ERROR_14FC06C9,
          new RuntimeException(CommonMessages.UNRECOVERABLE_ERROR));
      System.exit(-1);
    }
  }
}
