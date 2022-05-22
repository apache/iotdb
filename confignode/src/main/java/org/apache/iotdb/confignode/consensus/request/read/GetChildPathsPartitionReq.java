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

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.confignode.consensus.request.ConfigRequest;
import org.apache.iotdb.confignode.consensus.request.ConfigRequestType;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class GetChildPathsPartitionReq extends ConfigRequest {
  private PartialPath partialPath;

  public GetChildPathsPartitionReq() {
    super(ConfigRequestType.GetChildPathsPartition);
  }

  public PartialPath getPartialPath() {
    return partialPath;
  }

  public void setPartialPath(PartialPath partialPath) {
    this.partialPath = partialPath;
  }

  @Override
  protected void serializeImpl(ByteBuffer buffer) {
    partialPath.serialize(buffer);
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    partialPath = PartialPath.deserialize(buffer);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    GetChildPathsPartitionReq that = (GetChildPathsPartitionReq) o;
    return partialPath.equals(that.partialPath);
  }

  @Override
  public int hashCode() {
    return Objects.hash(partialPath);
  }
}
