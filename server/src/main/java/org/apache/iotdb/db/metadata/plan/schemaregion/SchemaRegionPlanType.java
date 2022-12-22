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

package org.apache.iotdb.db.metadata.plan.schemaregion;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public enum SchemaRegionPlanType {

  // region These PlanType shall keep consistent with the PhysicalPlanType.ordinal to ensure
  // compatibility
  CREATE_TIMESERIES((byte) 4),
  DELETE_TIMESERIES((byte) 21),
  CHANGE_TAG_OFFSET((byte) 28),
  CHANGE_ALIAS((byte) 29),
  //  SET_TEMPLATE((byte) 38),
  //  ACTIVATE_TEMPLATE((byte) 39),
  AUTO_CREATE_DEVICE_MNODE((byte) 40),
  CREATE_ALIGNED_TIMESERIES((byte) 41),
  //  UNSET_TEMPLATE((byte) 57),
  ACTIVATE_TEMPLATE_IN_CLUSTER((byte) 63),
  PRE_DELETE_TIMESERIES_IN_CLUSTER((byte) 64),
  ROLLBACK_PRE_DELETE_TIMESERIES((byte) 65),
  // endregion

  PRE_DEACTIVATE_TEMPLATE((byte) 0),
  ROLLBACK_PRE_DEACTIVATE_TEMPLATE((byte) 1),
  DEACTIVATE_TEMPLATE((byte) 2),

  // query plan doesn't need any ser/deSer, thus use one type to represent all
  READ_SCHEMA(Byte.MAX_VALUE);

  public static final int MAX_NUM = Byte.MAX_VALUE + 1;
  private static final SchemaRegionPlanType[] PLAN_TYPE_TABLE = new SchemaRegionPlanType[MAX_NUM];

  static {
    for (SchemaRegionPlanType type : SchemaRegionPlanType.values()) {
      PLAN_TYPE_TABLE[type.planType] = type;
    }
  }

  private final byte planType;

  SchemaRegionPlanType(byte planType) {
    this.planType = planType;
  }

  public byte getPlanType() {
    return planType;
  }

  public void serialize(DataOutputStream dataOutputStream) throws IOException {
    dataOutputStream.writeByte(planType);
  }

  public static SchemaRegionPlanType deserialize(ByteBuffer buffer) {
    byte code = buffer.get();
    SchemaRegionPlanType type = PLAN_TYPE_TABLE[code];
    if (type == null) {
      throw new IllegalArgumentException("Unrecognized SchemaRegionPlanType of " + code);
    }
    return type;
  }
}
