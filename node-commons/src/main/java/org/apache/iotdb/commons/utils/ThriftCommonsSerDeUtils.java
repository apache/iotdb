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

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.exception.runtime.ThriftSerDeException;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

/** Utils for serialize and deserialize all the data struct defined by thrift-commons */
public class ThriftCommonsSerDeUtils {

  private ThriftCommonsSerDeUtils() {
    // Empty constructor
  }

  private static TBinaryProtocol generateWriteProtocol(ByteArrayOutputStream outputStream)
      throws TTransportException {
    TTransport transport = new TIOStreamTransport(outputStream);
    return new TBinaryProtocol(transport);
  }

  private static TBinaryProtocol generateReadProtocol(ByteArrayInputStream inputStream)
      throws TTransportException {
    TTransport transport = new TIOStreamTransport(inputStream);
    return new TBinaryProtocol(transport);
  }

  public static void writeTEndPoint(TEndPoint endPoint, ByteBuffer buffer) {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    try {
      endPoint.write(generateWriteProtocol(outputStream));
    } catch (TException e) {
      throw new ThriftSerDeException("Write TEndPoint failed: ", e);
    }
    buffer.put(outputStream.toByteArray());
  }

  public static TEndPoint readTEndPoint(ByteBuffer buffer) {
    TEndPoint endPoint = new TEndPoint();
    ByteArrayInputStream inputStream =
        new ByteArrayInputStream(buffer.array(), buffer.position(), buffer.remaining());
    try {
      endPoint.read(generateReadProtocol(inputStream));
    } catch (TException e) {
      throw new ThriftSerDeException("Read TEndPoint failed: ", e);
    }
    buffer.position(buffer.limit() - inputStream.available());
    return endPoint;
  }

  public static void writeTDataNodeLocation(TDataNodeLocation dataNodeLocation, ByteBuffer buffer) {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    try {
      dataNodeLocation.write(generateWriteProtocol(outputStream));
    } catch (TException e) {
      throw new ThriftSerDeException("Write TDataNodeLocation failed: ", e);
    }
    buffer.put(outputStream.toByteArray());
  }

  public static TDataNodeLocation readTDataNodeLocation(ByteBuffer buffer) {
    TDataNodeLocation dataNodeLocation = new TDataNodeLocation();
    ByteArrayInputStream inputStream =
        new ByteArrayInputStream(buffer.array(), buffer.position(), buffer.remaining());
    try {
      dataNodeLocation.read(generateReadProtocol(inputStream));
    } catch (TException e) {
      throw new ThriftSerDeException("Read TDataNodeLocation failed: ", e);
    }
    buffer.position(buffer.limit() - inputStream.available());
    return dataNodeLocation;
  }

  public static void writeTSeriesPartitionSlot(
      TSeriesPartitionSlot seriesPartitionSlot, ByteBuffer buffer) {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    try {
      seriesPartitionSlot.write(generateWriteProtocol(outputStream));
    } catch (TException e) {
      throw new ThriftSerDeException("Write TSeriesPartitionSlot failed: ", e);
    }
    buffer.put(outputStream.toByteArray());
  }

  public static TSeriesPartitionSlot readTSeriesPartitionSlot(ByteBuffer buffer) {
    TSeriesPartitionSlot seriesPartitionSlot = new TSeriesPartitionSlot();
    ByteArrayInputStream inputStream =
        new ByteArrayInputStream(buffer.array(), buffer.position(), buffer.remaining());
    try {
      seriesPartitionSlot.read(generateReadProtocol(inputStream));
    } catch (TException e) {
      throw new ThriftSerDeException("Read TSeriesPartitionSlot failed: ", e);
    }
    buffer.position(buffer.limit() - inputStream.available());
    return seriesPartitionSlot;
  }

  public static void writeTTimePartitionSlot(
      TTimePartitionSlot timePartitionSlot, ByteBuffer buffer) {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    try {
      timePartitionSlot.write(generateWriteProtocol(outputStream));
    } catch (TException e) {
      throw new ThriftSerDeException("Write TTimePartitionSlot failed: ", e);
    }
    buffer.put(outputStream.toByteArray());
  }

  public static TTimePartitionSlot readTTimePartitionSlot(ByteBuffer buffer) {
    TTimePartitionSlot timePartitionSlot = new TTimePartitionSlot();
    ByteArrayInputStream inputStream =
        new ByteArrayInputStream(buffer.array(), buffer.position(), buffer.remaining());
    try {
      timePartitionSlot.read(generateReadProtocol(inputStream));
    } catch (TException e) {
      throw new ThriftSerDeException("Read TTimePartitionSlot failed: ", e);
    }
    buffer.position(buffer.limit() - inputStream.available());
    return timePartitionSlot;
  }

  public static void writeTConsensusGroupId(TConsensusGroupId consensusGroupId, ByteBuffer buffer) {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    try {
      consensusGroupId.write(generateWriteProtocol(outputStream));
    } catch (TException e) {
      throw new ThriftSerDeException("Write TConsensusGroupId failed: ", e);
    }
    buffer.put(outputStream.toByteArray());
  }

  public static TConsensusGroupId readTConsensusGroupId(ByteBuffer buffer) {
    TConsensusGroupId consensusGroupId = new TConsensusGroupId();
    ByteArrayInputStream inputStream =
        new ByteArrayInputStream(buffer.array(), buffer.position(), buffer.remaining());
    try {
      consensusGroupId.read(generateReadProtocol(inputStream));
    } catch (TException e) {
      throw new ThriftSerDeException("Read TConsensusGroupId failed: ", e);
    }
    buffer.position(buffer.limit() - inputStream.available());
    return consensusGroupId;
  }

  public static void writeTRegionReplicaSet(TRegionReplicaSet regionReplicaSet, ByteBuffer buffer) {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    try {
      regionReplicaSet.write(generateWriteProtocol(outputStream));
    } catch (TException e) {
      throw new ThriftSerDeException("Write TRegionReplicaSet failed: ", e);
    }
    buffer.put(outputStream.toByteArray());
  }

  public static TRegionReplicaSet readTRegionReplicaSet(ByteBuffer buffer) {
    TRegionReplicaSet regionReplicaSet = new TRegionReplicaSet();
    ByteArrayInputStream inputStream =
        new ByteArrayInputStream(buffer.array(), buffer.position(), buffer.remaining());
    try {
      regionReplicaSet.read(generateReadProtocol(inputStream));
    } catch (TException e) {
      throw new ThriftSerDeException("Read TRegionReplicaSet failed: ", e);
    }
    buffer.position(buffer.limit() - inputStream.available());
    return regionReplicaSet;
  }
}
