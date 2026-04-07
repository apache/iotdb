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

package org.apache.iotdb.commons.partition;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Objects;

public class DatabaseScopedDataPartitionTable {
  private final String database;
  private DataPartitionTable dataPartitionTable;

  public DatabaseScopedDataPartitionTable(String database, DataPartitionTable dataPartitionTable) {
    this.database = database;
    this.dataPartitionTable = dataPartitionTable;
  }

  public String getDatabase() {
    return database;
  }

  public DataPartitionTable getDataPartitionTable() {
    return dataPartitionTable;
  }

  public void serialize(OutputStream outputStream, TProtocol protocol)
      throws IOException, TException {
    ReadWriteIOUtils.write(database, outputStream);

    ReadWriteIOUtils.write(dataPartitionTable != null, outputStream);

    if (dataPartitionTable != null) {
      dataPartitionTable.serialize(outputStream, protocol);
    }
  }

  public static DatabaseScopedDataPartitionTable deserialize(ByteBuffer buffer) {
    String database = ReadWriteIOUtils.readString(buffer);

    boolean hasDataPartitionTable = ReadWriteIOUtils.readBool(buffer);

    DataPartitionTable dataPartitionTable = null;
    if (hasDataPartitionTable) {
      dataPartitionTable = new DataPartitionTable();
      dataPartitionTable.deserialize(buffer);
    }

    return new DatabaseScopedDataPartitionTable(database, dataPartitionTable);
  }

  public static DatabaseScopedDataPartitionTable deserialize(
      InputStream inputStream, TProtocol protocol) throws IOException, TException {
    String database = ReadWriteIOUtils.readString(inputStream);

    boolean hasDataPartitionTable = ReadWriteIOUtils.readBool(inputStream);

    DataPartitionTable dataPartitionTable = null;
    if (hasDataPartitionTable) {
      dataPartitionTable = new DataPartitionTable();
      dataPartitionTable.deserialize(inputStream, protocol);
    }

    return new DatabaseScopedDataPartitionTable(database, dataPartitionTable);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    DatabaseScopedDataPartitionTable that = (DatabaseScopedDataPartitionTable) o;
    return Objects.equals(database, that.database)
        && Objects.equals(dataPartitionTable, that.dataPartitionTable);
  }

  @Override
  public int hashCode() {
    return Objects.hash(database, dataPartitionTable);
  }
}
