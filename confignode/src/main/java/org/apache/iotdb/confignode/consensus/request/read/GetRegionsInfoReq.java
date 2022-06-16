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

import org.apache.iotdb.confignode.consensus.request.ConfigRequest;
import org.apache.iotdb.confignode.consensus.request.ConfigRequestType;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class GetRegionsInfoReq extends ConfigRequest {

  private int regionType;

  public GetRegionsInfoReq() {
    super(ConfigRequestType.GetRegionsInfo);
  }

  public GetRegionsInfoReq(int regionType) {
    super(ConfigRequestType.GetRegionsInfo);
    this.regionType = regionType;
  }

  public int getRegionType() {
    return regionType;
  }

  public void setRegionType(int regionType) {
    this.regionType = regionType;
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    stream.writeInt(getType().ordinal());
    ReadWriteIOUtils.write(regionType, stream);
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    regionType = ReadWriteIOUtils.readInt(buffer);
  }
}
