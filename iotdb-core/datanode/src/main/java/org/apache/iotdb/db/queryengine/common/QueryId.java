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

package org.apache.iotdb.db.queryengine.common;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class QueryId {

  public static final QueryId MOCK_QUERY_ID = QueryId.valueOf("mock_query_id");

  private static final int DATANODE_ID = IoTDBDescriptor.getInstance().getConfig().getDataNodeId();

  private final String id;

  private int nextPlanNodeIndex;
  private int nextPlanFragmentIndex;

  private static final String INVALID_ID_ERROR_MSG = "Invalid id %s";

  public static QueryId valueOf(String queryId) {
    // ID is verified in the constructor
    return new QueryId(queryId);
  }

  public QueryId(String id) {
    this.id = validateId(id);
    this.nextPlanNodeIndex = 0;
    this.nextPlanFragmentIndex = 0;
  }

  public PlanNodeId genPlanNodeId() {
    return new PlanNodeId(String.valueOf(nextPlanNodeIndex++));
  }

  public PlanFragmentId genPlanFragmentId() {
    return new PlanFragmentId(this, nextPlanFragmentIndex++);
  }

  public String getId() {
    return id;
  }

  public static int getDataNodeId() {
    return DATANODE_ID;
  }

  @Override
  public String toString() {
    return id;
  }

  @Override
  public int hashCode() {
    return Objects.hash(id);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    QueryId other = (QueryId) obj;
    return Objects.equals(this.id, other.id);
  }

  public static List<String> parseDottedId(String id, int expectedParts, String name) {
    requireNonNull(id, "id is null");
    checkArgument(expectedParts > 0, "expectedParts must be at least 1");
    requireNonNull(name, "name is null");

    List<String> ids = Arrays.asList(id.split("\\."));
    checkArgument(ids.size() == expectedParts, "Invalid %s %s", name, id);

    for (String part : ids) {
      checkArgument(!part.isEmpty(), INVALID_ID_ERROR_MSG, id);
      checkArgument(isValidId(part), INVALID_ID_ERROR_MSG, id);
    }
    return ids;
  }

  private static void checkArgument(boolean condition, String message, Object... messageArgs) {
    if (!condition) {
      throw new IllegalArgumentException(format(message, messageArgs));
    }
  }

  //
  // Id helper methods
  //

  // Check if the string matches [_a-z0-9]+ , but without the overhead of regex
  private static boolean isValidId(String id) {
    if (id.length() == 0) {
      return false;
    }

    for (int i = 0; i < id.length(); i++) {
      char c = id.charAt(i);
      if (!(c == '_' || c >= 'a' && c <= 'z' || c >= '0' && c <= '9')) {
        return false;
      }
    }
    return true;
  }

  public static String validateId(String id) {
    requireNonNull(id, "id is null");
    checkArgument(!id.isEmpty(), "id is empty");
    checkArgument(isValidId(id), INVALID_ID_ERROR_MSG, id);
    return id;
  }

  public void serialize(ByteBuffer byteBuffer) {
    ReadWriteIOUtils.write(this.id, byteBuffer);
  }

  public void serialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(this.id, stream);
  }

  public static QueryId deserialize(ByteBuffer byteBuffer) {
    return new QueryId(ReadWriteIOUtils.readString(byteBuffer));
  }
}
