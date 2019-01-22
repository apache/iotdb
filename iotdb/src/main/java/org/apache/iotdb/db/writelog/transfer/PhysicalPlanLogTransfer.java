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
package org.apache.iotdb.db.writelog.transfer;

import java.io.IOException;
import java.nio.BufferOverflowException;
import org.apache.iotdb.db.exception.WALOverSizedException;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;

public class PhysicalPlanLogTransfer {

  public static byte[] operatorToLog(PhysicalPlan plan) throws WALOverSizedException {
    Codec<PhysicalPlan> codec = null;
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
      default:
        throw new UnsupportedOperationException(
            "SystemLogOperator given is not supported. " + plan.getOperatorType());
    }
    try {
      return codec.encode(plan);
    } catch (BufferOverflowException e) {
      throw new WALOverSizedException("Plan " + plan.toString() + " is too big to write to WAL");
    }
  }

  public static PhysicalPlan logToOperator(byte[] opInBytes) throws IOException {
    // the first byte determines the opCode
    int opCode = opInBytes[0];
    Codec<PhysicalPlan> codec = (Codec<PhysicalPlan>) PhysicalPlanCodec.fromOpcode(opCode).codec;
    return codec.decode(opInBytes);
  }
}
