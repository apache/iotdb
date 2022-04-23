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

import org.apache.iotdb.commons.exception.runtime.ThriftSerDeException;
import org.apache.iotdb.confignode.rpc.thrift.TStorageGroupSchema;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TByteBuffer;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

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

  public static void writeTStorageGroupSchema(
      TStorageGroupSchema storageGroupSchema, ByteBuffer buffer) {
    try {
      storageGroupSchema.write(generateWriteProtocol(buffer));
    } catch (TException e) {
      throw new ThriftSerDeException("Write TStorageGroupSchema failed: ", e);
    }
  }

  public static TStorageGroupSchema readTStorageGroupSchema(ByteBuffer buffer) {
    TStorageGroupSchema storageGroupSchema = new TStorageGroupSchema();
    try {
      storageGroupSchema.read(generateReadProtocol(buffer));
    } catch (TException e) {
      throw new ThriftSerDeException("Read TStorageGroupSchema failed: ", e);
    }
    return storageGroupSchema;
  }
}
