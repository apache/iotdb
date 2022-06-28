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
package org.apache.iotdb.confignode.consensus.request.read;

import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.commons.utils.ThriftConfigNodeSerDeUtils;
import org.apache.iotdb.confignode.consensus.request.ConfigRequest;
import org.apache.iotdb.confignode.consensus.request.ConfigRequestType;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class GetConfigNodeConfigurationReq extends ConfigRequest {
  private TConfigNodeLocation configNodeLocation;

  public GetConfigNodeConfigurationReq() {
    super(ConfigRequestType.GetConfigNodeConfiguration);
  }

  public GetConfigNodeConfigurationReq(TConfigNodeLocation configNodeLocation) {
    this();
    this.configNodeLocation = configNodeLocation;
  }

  public TConfigNodeLocation getConfigNodeLocation() {
    return configNodeLocation;
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(ConfigRequestType.GetConfigNodeConfiguration.ordinal(), stream);

    ThriftConfigNodeSerDeUtils.serializeTConfigNodeLocation(configNodeLocation, stream);
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    configNodeLocation = ThriftConfigNodeSerDeUtils.deserializeTConfigNodeLocation(buffer);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    GetConfigNodeConfigurationReq that = (GetConfigNodeConfigurationReq) o;
    return configNodeLocation.equals(that.configNodeLocation);
  }

  @Override
  public int hashCode() {
    return Objects.hash(configNodeLocation);
  }
}
