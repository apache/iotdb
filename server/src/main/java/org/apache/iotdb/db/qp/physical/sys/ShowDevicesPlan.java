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

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class ShowDevicesPlan extends ShowPlan {

  public ShowDevicesPlan() {
    super(ShowContentType.DEVICES);
  }

  private boolean hasSgCol;

  public ShowDevicesPlan(PartialPath path) {
    super(ShowContentType.DEVICES, path);
  }

  public ShowDevicesPlan(PartialPath path, int limit, int offset, int fetchSize, boolean hasSgCol) {
    super(ShowContentType.DEVICES, path, limit, offset, fetchSize);
    this.hasSgCol = hasSgCol;
  }

  @Override
  public void serialize(DataOutputStream outputStream) throws IOException {
    outputStream.write(PhysicalPlanType.SHOW_DEVICES.ordinal());
    putString(outputStream, path.getFullPath());
    outputStream.writeInt(limit);
    outputStream.writeInt(offset);
    outputStream.writeLong(index);
  }

  @Override
  public void deserialize(ByteBuffer buffer) throws IllegalPathException {
    path = new PartialPath(readString(buffer));
    limit = buffer.getInt();
    offset = buffer.getInt();
    this.index = buffer.getLong();
  }

  public boolean hasSgCol() {
    return hasSgCol;
  }
}
