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
package org.apache.iotdb.confignode.persistence.partition;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

public class RegionInfo {

  private final TConsensusGroupId id;
  private final TRegionReplicaSet replicaSet;

  private final AtomicLong partitionCount;

  public RegionInfo() {
    this.id = new TConsensusGroupId();
    this.replicaSet = new TRegionReplicaSet();
    this.partitionCount = new AtomicLong();
  }

  public RegionInfo(TRegionReplicaSet replicaSet) {
    this.id = replicaSet.getRegionId();
    this.replicaSet = replicaSet;
    this.partitionCount = new AtomicLong(0);
  }

  public TConsensusGroupId getId() {
    return id;
  }

  public TRegionReplicaSet getReplicaSet() {
    return replicaSet;
  }

  public void addCounter(long delta) {
    partitionCount.getAndAdd(delta);
  }

  public long getCounter() {
    return partitionCount.get();
  }

  public void serialize(OutputStream outputStream, TProtocol protocol)
      throws IOException, TException {
    id.write(protocol);
    replicaSet.write(protocol);
    ReadWriteIOUtils.write(partitionCount.get(), outputStream);
  }

  public void deserialize(InputStream inputStream, TProtocol protocol)
      throws IOException, TException {
    id.read(protocol);
    replicaSet.read(protocol);
    partitionCount.set(ReadWriteIOUtils.readLong(inputStream));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    RegionInfo that = (RegionInfo) o;
    return id.equals(that.id) && replicaSet.equals(that.replicaSet);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, replicaSet);
  }
}
