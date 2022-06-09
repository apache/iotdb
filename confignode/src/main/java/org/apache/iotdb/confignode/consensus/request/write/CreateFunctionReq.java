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

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class CreateFunctionReq extends ConfigRequest {

  private String functionName;
  private String className;
  private List<String> uris;

  public CreateFunctionReq() {
    super(ConfigRequestType.CreateFunction);
  }

  public CreateFunctionReq(String functionName, String className, List<String> uris) {
    super(ConfigRequestType.CreateFunction);
    this.functionName = functionName;
    this.className = className;
    this.uris = uris;
  }

  public String getFunctionName() {
    return functionName;
  }

  public String getClassName() {
    return className;
  }

  public List<String> getUris() {
    return uris;
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    stream.writeInt(getType().ordinal());

    ReadWriteIOUtils.write(functionName, stream);
    ReadWriteIOUtils.write(className, stream);

    final int size = uris.size();
    ReadWriteIOUtils.write(size, stream);
    for (String uri : uris) {
      ReadWriteIOUtils.write(uri, stream);
    }
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    functionName = ReadWriteIOUtils.readString(buffer);
    className = ReadWriteIOUtils.readString(buffer);

    final int size = ReadWriteIOUtils.readInt(buffer);
    uris = new ArrayList<>(size);
    for (int i = 0; i < size; ++i) {
      uris.add(ReadWriteIOUtils.readString(buffer));
    }
  }
}
