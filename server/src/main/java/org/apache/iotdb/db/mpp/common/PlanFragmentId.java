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

import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class PlanFragmentId {

  private final QueryId queryId;
  private final int id;

  private int nextFragmentInstanceId;

  public static PlanFragmentId valueOf(String stageId) {
    List<String> ids = QueryId.parseDottedId(stageId, 2, "stageId");
    return valueOf(ids);
  }

  public static PlanFragmentId valueOf(List<String> ids) {
    checkArgument(ids.size() == 2, "Expected two ids but got: %s", ids);
    return new PlanFragmentId(new QueryId(ids.get(0)), Integer.parseInt(ids.get(1)));
  }

  public PlanFragmentId(String queryId, int id) {
    this(new QueryId(queryId), id);
  }

  public PlanFragmentId(QueryId queryId, int id) {
    this.queryId = requireNonNull(queryId, "queryId is null");
    this.id = id;
    this.nextFragmentInstanceId = 0;
  }

  public FragmentInstanceId genFragmentInstanceId() {
    return new FragmentInstanceId(this, String.valueOf(nextFragmentInstanceId++));
  }

  public QueryId getQueryId() {
    return queryId;
  }

  public int getId() {
    return id;
  }

  public String toString() {
    return String.format("%s.%d", queryId, id);
  }

  public void serialize(ByteBuffer byteBuffer) {
    queryId.serialize(byteBuffer);
    ReadWriteIOUtils.write(id, byteBuffer);
  }

  public void serialize(DataOutputStream stream) throws IOException {
    queryId.serialize(stream);
    ReadWriteIOUtils.write(id, stream);
  }

  public static PlanFragmentId deserialize(ByteBuffer byteBuffer) {
    return new PlanFragmentId(
        QueryId.deserialize(byteBuffer), ReadWriteIOUtils.readInt(byteBuffer));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PlanFragmentId that = (PlanFragmentId) o;
    return id == that.id && Objects.equals(queryId, that.queryId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(queryId, id);
  }
}
