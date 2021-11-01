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

import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.logical.Operator.OperatorType;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class CreateFunctionPlan extends PhysicalPlan {

  private String udfName;
  private String className;

  public CreateFunctionPlan() {
    super(false, OperatorType.CREATE_FUNCTION);
  }

  public CreateFunctionPlan(String udfName, String className) {
    super(false, OperatorType.CREATE_FUNCTION);
    this.udfName = udfName;
    this.className = className;
  }

  public String getUdfName() {
    return udfName;
  }

  public String getClassName() {
    return className;
  }

  public void setClassName(String className) {
    this.className = className;
  }

  @Override
  public List<PartialPath> getPaths() {
    return new ArrayList<>();
  }

  @Override
  public void serialize(DataOutputStream outputStream) throws IOException {
    outputStream.writeByte((byte) PhysicalPlanType.CREATE_FUNCTION.ordinal());

    putString(outputStream, udfName);
    putString(outputStream, className);
    outputStream.writeLong(index);
  }

  @Override
  public void deserialize(ByteBuffer buffer) throws IllegalPathException {

    udfName = readString(buffer);
    className = readString(buffer);
    this.index = buffer.getLong();
  }
}
