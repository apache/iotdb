/**
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
package org.apache.iotdb.db.qp.physical.transfer;

import java.io.IOException;
import java.nio.BufferOverflowException;
import org.apache.iotdb.db.exception.WALOverSizedException;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;

public class PhysicalPlanLogTransfer {

  private PhysicalPlanLogTransfer() {
  }

  public static byte[] planToLog(PhysicalPlan plan) throws IOException {
    Codec<PhysicalPlan> codec;
    switch (plan.getOperatorType()) {
      case INSERT:
        codec = (Codec<PhysicalPlan>) PhysicalPlanCodec.fromOpcode(SystemLogOperator.INSERT).codec;
        break;
      case UPDATE:
        codec = (Codec<PhysicalPlan>) PhysicalPlanCodec.fromOpcode(SystemLogOperator.UPDATE).codec;
        break;
      case DELETE:
        codec = (Codec<PhysicalPlan>) PhysicalPlanCodec.fromOpcode(SystemLogOperator.DELETE).codec;
        break;
      case DELETE_TIMESERIES:
      case CREATE_TIMESERIES:
      case SET_STORAGE_GROUP:
        codec = (Codec<PhysicalPlan>) PhysicalPlanCodec.fromOpcode(SystemLogOperator.METADATA).codec;
        break;
      case AUTHOR:
      case CREATE_USER:
      case CREATE_ROLE:
      case DELETE_ROLE:
      case DELETE_USER:
      case GRANT_USER_ROLE:
      case GRANT_USER_PRIVILEGE:
      case REVOKE_USER_PRIVILEGE:
      case REVOKE_USER_ROLE:
      case GRANT_ROLE_PRIVILEGE:
      case LIST_USER:
      case LIST_ROLE:
      case LIST_USER_PRIVILEGE:
      case LIST_ROLE_PRIVILEGE:
      case LIST_USER_ROLES:
      case LIST_ROLE_USERS:
      case MODIFY_PASSWORD:
        codec = (Codec<PhysicalPlan>) PhysicalPlanCodec.fromOpcode(SystemLogOperator.AUTHOR).codec;
        break;
      case LOADDATA:
        codec = (Codec<PhysicalPlan>) PhysicalPlanCodec.fromOpcode(SystemLogOperator.LOADDATA).codec;
        break;
      case PROPERTY:
        codec = (Codec<PhysicalPlan>) PhysicalPlanCodec.fromOpcode(SystemLogOperator.PROPERTY).codec;
        break;
      default:
        throw new UnsupportedOperationException(
            "SystemLogOperator given is not supported. " + plan.getOperatorType());
    }
    try {
      return codec.encode(plan);
    } catch (BufferOverflowException e) {
      throw new WALOverSizedException("Plan " + plan.toString() + " is too big to insert to WAL");
    }
  }

  public static PhysicalPlan logToPlan(byte[] opInBytes) throws IOException {
    // the first byte determines the opCode
    int opCode = opInBytes[0];
    Codec<PhysicalPlan> codec = (Codec<PhysicalPlan>) PhysicalPlanCodec.fromOpcode(opCode).codec;
    return codec.decode(opInBytes);
  }
}
