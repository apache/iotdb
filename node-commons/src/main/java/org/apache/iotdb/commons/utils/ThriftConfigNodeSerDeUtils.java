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

import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.commons.exception.runtime.ThriftSerDeException;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;
import org.apache.iotdb.confignode.rpc.thrift.TPipeSinkInfo;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TByteBuffer;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.io.DataOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/** Utils for serialize and deserialize all the data struct defined by thrift-confignode */
public class ThriftConfigNodeSerDeUtils {

  private ThriftConfigNodeSerDeUtils() {
    // Empty constructor
  }

  private static TBinaryProtocol generateWriteProtocol(ByteBuffer buffer)
      throws TTransportException {
    TTransport transport = new TByteBuffer(buffer);
    return new TBinaryProtocol(transport);
  }

  private static TBinaryProtocol generateReadProtocol(ByteBuffer buffer)
      throws TTransportException {
    TTransport transport = new TByteBuffer(buffer);
    return new TBinaryProtocol(transport);
  }

  private static TBinaryProtocol generateWriteProtocol(OutputStream outputStream)
      throws TTransportException {
    TIOStreamTransport tioStreamTransport = new TIOStreamTransport(outputStream);
    return new TBinaryProtocol(tioStreamTransport);
  }

  private static TBinaryProtocol generateReadProtocol(InputStream inputStream)
      throws TTransportException {
    TIOStreamTransport tioStreamTransport = new TIOStreamTransport(inputStream);
    return new TBinaryProtocol(tioStreamTransport);
  }

  public static void serializeTStorageGroupSchema(
      TDatabaseSchema storageGroupSchema, ByteBuffer buffer) {
    try {
      storageGroupSchema.write(generateWriteProtocol(buffer));
    } catch (TException e) {
      throw new ThriftSerDeException("Write TStorageGroupSchema failed: ", e);
    }
  }

  public static TDatabaseSchema deserializeTStorageGroupSchema(ByteBuffer buffer) {
    TDatabaseSchema storageGroupSchema = new TDatabaseSchema();
    try {
      storageGroupSchema.read(generateReadProtocol(buffer));
    } catch (TException e) {
      throw new ThriftSerDeException("Read TStorageGroupSchema failed: ", e);
    }
    return storageGroupSchema;
  }

  public static void serializeTStorageGroupSchema(
      TDatabaseSchema storageGroupSchema, OutputStream outputStream) {
    try {
      storageGroupSchema.write(generateWriteProtocol(outputStream));
    } catch (TException e) {
      throw new ThriftSerDeException("Write TStorageGroupSchema failed: ", e);
    }
  }

  public static TDatabaseSchema deserializeTStorageGroupSchema(InputStream inputStream) {
    TDatabaseSchema storageGroupSchema = new TDatabaseSchema();
    try {
      storageGroupSchema.read(generateReadProtocol(inputStream));
    } catch (TException e) {
      throw new ThriftSerDeException("Read TStorageGroupSchema failed: ", e);
    }
    return storageGroupSchema;
  }

  public static void serializeTConfigNodeLocation(
      TConfigNodeLocation configNodeLocation, ByteBuffer buffer) {
    try {
      configNodeLocation.write(generateWriteProtocol(buffer));
    } catch (TException e) {
      throw new ThriftSerDeException("Write TConfigNodeLocation failed: ", e);
    }
  }

  public static void serializeTConfigNodeLocation(
      TConfigNodeLocation configNodeLocation, DataOutputStream stream) {
    try {
      configNodeLocation.write(generateWriteProtocol(stream));
    } catch (TException e) {
      throw new ThriftSerDeException("Write TConfigNodeLocation failed: ", e);
    }
  }

  public static TConfigNodeLocation deserializeTConfigNodeLocation(ByteBuffer buffer) {
    TConfigNodeLocation configNodeLocation = new TConfigNodeLocation();
    try {
      configNodeLocation.read(generateReadProtocol(buffer));
    } catch (TException e) {
      throw new ThriftSerDeException("Read TConfigNodeLocation failed: ", e);
    }
    return configNodeLocation;
  }

  public static void serializeTPipeSinkInfo(TPipeSinkInfo pipeSinkInfo, DataOutputStream stream) {
    try {
      pipeSinkInfo.write(generateWriteProtocol(stream));
    } catch (TException e) {
      throw new ThriftSerDeException("Write TPipeSinkInfo failed: ", e);
    }
  }

  public static TPipeSinkInfo deserializeTPipeSinkInfo(ByteBuffer buffer) {
    TPipeSinkInfo pipeSinkInfo = new TPipeSinkInfo();
    try {
      pipeSinkInfo.read(generateReadProtocol(buffer));
    } catch (TException e) {
      throw new ThriftSerDeException("Read TPipeSinkInfo failed: ", e);
    }
    return pipeSinkInfo;
  }
}
