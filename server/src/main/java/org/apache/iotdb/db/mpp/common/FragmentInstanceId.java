/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.mpp.common;

import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

/** The fragment instance ID class. */
public class FragmentInstanceId {

  private final String fullId;
  private final QueryId queryId;
  private final PlanFragmentId fragmentId;
  private final String instanceId;

  public FragmentInstanceId(PlanFragmentId fragmentId, String instanceId) {
    this.queryId = fragmentId.getQueryId();
    this.fragmentId = fragmentId;
    this.instanceId = instanceId;
    this.fullId = createFullId(fragmentId.getQueryId().getId(), fragmentId.getId(), instanceId);
  }

  public String getFullId() {
    return fullId;
  }

  public QueryId getQueryId() {
    return queryId;
  }

  public PlanFragmentId getFragmentId() {
    return fragmentId;
  }

  public String getInstanceId() {
    return instanceId;
  }

  public String toString() {
    return fullId;
  }

  public static FragmentInstanceId deserialize(ByteBuffer byteBuffer) {
    return new FragmentInstanceId(
        PlanFragmentId.deserialize(byteBuffer), ReadWriteIOUtils.readString(byteBuffer));
  }

  public void serialize(ByteBuffer byteBuffer) {
    fragmentId.serialize(byteBuffer);
    ReadWriteIOUtils.write(instanceId, byteBuffer);
  }

  public void serialize(DataOutputStream stream) throws IOException {
    fragmentId.serialize(stream);
    ReadWriteIOUtils.write(instanceId, stream);
  }

  public TFragmentInstanceId toThrift() {
    return new TFragmentInstanceId(queryId.getId(), fragmentId.getId(), instanceId);
  }

  public static FragmentInstanceId fromThrift(TFragmentInstanceId tFragmentInstanceId) {
    return new FragmentInstanceId(
        new PlanFragmentId(tFragmentInstanceId.queryId, tFragmentInstanceId.fragmentId),
        tFragmentInstanceId.instanceId);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    FragmentInstanceId that = (FragmentInstanceId) o;
    return Objects.equals(fullId, that.fullId)
        && Objects.equals(queryId, that.queryId)
        && Objects.equals(fragmentId, that.fragmentId)
        && Objects.equals(instanceId, that.instanceId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(fullId, queryId, fragmentId, instanceId);
  }

  public static String createFullId(String queryId, int fragmentId, String instanceId) {
    return queryId + "." + fragmentId + "." + instanceId;
  }

  public static String createFragmentInstanceIdFromTFragmentInstanceId(
      TFragmentInstanceId tFragmentInstanceId) {
    return tFragmentInstanceId.getFragmentId() + "." + tFragmentInstanceId.getInstanceId();
  }
}
