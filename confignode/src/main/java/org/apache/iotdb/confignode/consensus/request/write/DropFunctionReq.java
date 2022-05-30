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

package org.apache.iotdb.confignode.consensus.request.write;

import org.apache.iotdb.confignode.consensus.request.ConfigRequest;
import org.apache.iotdb.confignode.consensus.request.ConfigRequestType;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.nio.ByteBuffer;

public class DropFunctionReq extends ConfigRequest {

  private String functionName;

  public DropFunctionReq() {
    super(ConfigRequestType.DropFunction);
  }

  public DropFunctionReq(String functionName) {
    super(ConfigRequestType.DropFunction);
    this.functionName = functionName;
  }

  public String getFunctionName() {
    return functionName;
  }

  @Override
  protected void serializeImpl(ByteBuffer buffer) {
    buffer.putInt(getType().ordinal());
    ReadWriteIOUtils.write(functionName, buffer);
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    functionName = ReadWriteIOUtils.readString(buffer);
  }
}
