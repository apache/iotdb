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
package org.apache.iotdb.commons.cluster;

import java.nio.ByteBuffer;

public class DataNodeLocation {

  private int dataNodeID;
  private Endpoint endPoint;

  public DataNodeLocation() {}

  public DataNodeLocation(int dataNodeID, Endpoint endPoint) {
    this.dataNodeID = dataNodeID;
    this.endPoint = endPoint;
  }

  public int getDataNodeID() {
    return dataNodeID;
  }

  public void setDataNodeID(int dataNodeID) {
    this.dataNodeID = dataNodeID;
  }

  public Endpoint getEndPoint() {
    return endPoint;
  }

  public void serializeImpl(ByteBuffer buffer) {
    buffer.putInt(dataNodeID);
    endPoint.serializeImpl(buffer);
  }

  public void deserializeImpl(ByteBuffer buffer) {
    dataNodeID = buffer.getInt();
    endPoint = new Endpoint();
    endPoint.deserializeImpl(buffer);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    return endPoint.equals(((DataNodeLocation) o).getEndPoint());
  }

  @Override
  public int hashCode() {
    return endPoint.hashCode();
  }

  public String toString() {
    return String.format("DataNode[%d, %s]", dataNodeID, endPoint);
  }
}
