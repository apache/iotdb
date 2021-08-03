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
package org.apache.iotdb.db.qp.physical.sys;

import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.logical.Operator.OperatorType;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class DataAuthPlan extends PhysicalPlan {

  private List<String> users;

  public DataAuthPlan(OperatorType operatorType) {
    super(false, operatorType);
  }

  public DataAuthPlan(OperatorType operatorType, List<String> users) {
    super(false, operatorType);
    this.users = users;
  }

  public List<String> getUsers() {
    return users;
  }

  @Override
  public List<PartialPath> getPaths() {
    return Collections.emptyList();
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    int type = this.getPlanType(super.getOperatorType());
    stream.writeByte((byte) type);
    stream.writeInt(users.size());

    for (String user : users) {
      putString(stream, user);
    }

    stream.writeLong(index);
  }

  @Override
  public void serialize(ByteBuffer buffer) {
    int type = this.getPlanType(super.getOperatorType());
    buffer.put((byte) type);
    buffer.putInt(users.size());

    for (String user : users) {
      putString(buffer, user);
    }

    buffer.putLong(index);
  }

  @Override
  public void deserialize(ByteBuffer buffer) {
    int userSize = buffer.getInt();
    this.users = new ArrayList<>(userSize);
    for (int i = 0; i < userSize; i++) {
      users.add(readString(buffer));
    }
  }

  private int getPlanType(OperatorType operatorType) {
    int type;
    if (operatorType == OperatorType.GRANT_WATERMARK_EMBEDDING) {
      type = PhysicalPlanType.GRANT_WATERMARK_EMBEDDING.ordinal();
    } else if (operatorType == OperatorType.REVOKE_WATERMARK_EMBEDDING) {
      type = PhysicalPlanType.REVOKE_WATERMARK_EMBEDDING.ordinal();
    } else {
      throw new IllegalArgumentException("Unknown operator: " + operatorType);
    }
    return type;
  }
}
