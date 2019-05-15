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
import java.util.HashMap;

public enum PhysicalPlanCodec {

  MULTIINSERTPLAN(SystemLogOperator.INSERT, CodecInstances.multiInsertPlanCodec),
  UPDATEPLAN(SystemLogOperator.UPDATE, CodecInstances.updatePlanCodec),
  DELETEPLAN(SystemLogOperator.DELETE, CodecInstances.deletePlanCodec),
  METADATAPLAN(SystemLogOperator.METADATA, CodecInstances.metadataPlanCodec),
  AUTHORPLAN(SystemLogOperator.AUTHOR, CodecInstances.authorPlanCodec),
  LOADDATAPLAN(SystemLogOperator.LOADDATA, CodecInstances.loadDataPlanCodec),
  PROPERTYPLAN(SystemLogOperator.PROPERTY, CodecInstances.propertyPlanCodec);

  private static final HashMap<Integer, PhysicalPlanCodec> codecMap = new HashMap<>();

  static {
    for (PhysicalPlanCodec codec : PhysicalPlanCodec.values()) {
      codecMap.put(codec.planCode, codec);
    }
  }

  public final int planCode;
  public final Codec<?> codec;

  PhysicalPlanCodec(int planCode, Codec<?> codec) {
    this.planCode = planCode;
    this.codec = codec;
  }

  public static PhysicalPlanCodec fromOpcode(int opcode) throws IOException {
    if (!codecMap.containsKey(opcode)) {
      throw new IOException(
          "SystemLogOperator [" + opcode + "] is not supported. ");
    }
    return codecMap.get(opcode);
  }
}