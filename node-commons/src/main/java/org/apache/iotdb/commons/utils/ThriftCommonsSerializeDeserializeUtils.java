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
package org.apache.iotdb.commons.utils;

import org.apache.iotdb.common.rpc.thrift.EndPoint;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;

import java.nio.ByteBuffer;

/** Utils for serialize and deserialize all the data struct defined by thrift-commons */
public class ThriftCommonsSerializeDeserializeUtils {

  private ThriftCommonsSerializeDeserializeUtils() {
    // Empty constructor
  }

  public static void writeEndPoint(EndPoint endPoint, ByteBuffer buffer) {
    BasicStructureSerializeDeserializeUtil.write(endPoint.getIp(), buffer);
    buffer.putInt(endPoint.getPort());
  }

  public static EndPoint readEndPoint(ByteBuffer buffer) {
    EndPoint endPoint = new EndPoint();
    endPoint.setIp(BasicStructureSerializeDeserializeUtil.readString(buffer));
    endPoint.setPort(buffer.getInt());
    return endPoint;
  }

  public static void writeTDataNodeLocation(TDataNodeLocation dataNodeLocation, ByteBuffer buffer) {
    buffer.putInt(dataNodeLocation.getDataNodeId());
    writeEndPoint(dataNodeLocation.getExternalEndPoint(), buffer);
    writeEndPoint(dataNodeLocation.getInternalEndPoint(), buffer);
    writeEndPoint(dataNodeLocation.getDataBlockManagerEndPoint(), buffer);
    writeEndPoint(dataNodeLocation.getConsensusEndPoint(), buffer);
  }

  public static TDataNodeLocation readTDataNodeLocation(ByteBuffer buffer) {
    TDataNodeLocation dataNodeLocation = new TDataNodeLocation();
    dataNodeLocation.setDataNodeId(buffer.getInt());
    dataNodeLocation.setExternalEndPoint(readEndPoint(buffer));
    dataNodeLocation.setInternalEndPoint(readEndPoint(buffer));
    dataNodeLocation.setDataBlockManagerEndPoint(readEndPoint(buffer));
    dataNodeLocation.setConsensusEndPoint(readEndPoint(buffer));
    return dataNodeLocation;
  }
}
