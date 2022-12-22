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
package org.apache.iotdb.db.qp.physical;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.runtime.SerializationRunTimeException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** This class is an abstract class for all type of PhysicalPlan. */
public abstract class PhysicalPlan implements IConsensusRequest {
  private static final Logger logger = LoggerFactory.getLogger(PhysicalPlan.class);

  private static final String SERIALIZATION_UNIMPLEMENTED = "serialization unimplemented";

  private boolean isQuery = false;

  private Operator.OperatorType operatorType;

  // for cluster mode, whether the plan may be splitted into several sub plans
  protected boolean canBeSplit = true;

  // login username, corresponding to cli/session login user info
  private String loginUserName;

  // a bridge from a cluster raft log to a physical plan
  protected long index;

  private boolean debug;

  /**
   * Since IoTDB v0.13, all DDL and DML use patternMatch as default. Before IoTDB v0.13, all DDL and
   * DML use prefixMatch.
   */
  private boolean isPrefixMatch = false;

  /** whether the plan can be split into more than one Plans. Only used in the cluster mode. */
  public boolean canBeSplit() {
    return canBeSplit;
  }

  protected PhysicalPlan() {}

  protected PhysicalPlan(Operator.OperatorType operatorType) {
    this.operatorType = operatorType;
  }

  public abstract List<? extends PartialPath> getPaths();

  public void setPaths(List<PartialPath> paths) {}

  public boolean isQuery() {
    return isQuery;
  }

  public boolean isSelectInto() {
    return false;
  }

  public Operator.OperatorType getOperatorType() {
    return operatorType;
  }

  public String getOperatorName() {
    return operatorType.toString();
  }

  public void setOperatorType(Operator.OperatorType operatorType) {
    this.operatorType = operatorType;
  }

  public List<String> getAggregations() {
    return Collections.emptyList();
  }

  public void setQuery(boolean query) {
    isQuery = query;
  }

  public boolean isDebug() {
    return debug;
  }

  public void setDebug(boolean debug) {
    this.debug = debug;
  }

  /**
   * Serialize the plan into the given buffer. All necessary fields will be serialized.
   *
   * @param stream
   * @throws IOException
   */
  public void serialize(DataOutputStream stream) throws IOException {
    throw new UnsupportedOperationException(SERIALIZATION_UNIMPLEMENTED);
  }

  @Override
  public ByteBuffer serializeToByteBuffer() {
    try (PublicBAOS byteArrayOutputStream = new PublicBAOS();
        DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      serialize(outputStream);
      return ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    } catch (IOException e) {
      logger.error("Unexpected error occurs when serializing this physical plan.", e);
      throw new SerializationRunTimeException(e);
    }
  }

  public void deserialize(DataInputStream stream) throws IOException, IllegalPathException {
    throw new UnsupportedOperationException(SERIALIZATION_UNIMPLEMENTED);
  }

  /**
   * Serialize the plan into the given buffer. This is provided for WAL, so fields that can be
   * recovered will not be serialized. If error occurs when serializing this plan, the buffer will
   * be reset.
   *
   * @param buffer
   */
  public final void serialize(ByteBuffer buffer) {
    buffer.mark();
    try {
      serializeImpl(buffer);
    } catch (UnsupportedOperationException e) {
      // ignore and throw
      throw e;
    } catch (BufferOverflowException e) {
      buffer.reset();
      throw e;
    } catch (Exception e) {
      logger.error(
          "Rollback buffer entry because error occurs when serializing this physical plan.", e);
      buffer.reset();
      throw e;
    }
  }

  protected void serializeImpl(ByteBuffer buffer) {
    throw new UnsupportedOperationException(SERIALIZATION_UNIMPLEMENTED);
  }

  protected void putString(ByteBuffer buffer, String value) {
    ReadWriteIOUtils.write(value, buffer);
  }

  protected void putStrings(ByteBuffer buffer, List<String> values) {
    for (String value : values) {
      putString(buffer, value);
    }
  }

  protected void putString(DataOutputStream stream, String value) throws IOException {
    ReadWriteIOUtils.write(value, stream);
  }

  protected void putStrings(DataOutputStream stream, List<String> values) throws IOException {
    for (String value : values) {
      putString(stream, value);
    }
  }

  protected String readString(ByteBuffer buffer) {
    return ReadWriteIOUtils.readString(buffer);
  }

  protected List<String> readStrings(ByteBuffer buffer, int totalSize) {
    List<String> result = new ArrayList<>(totalSize);
    for (int i = 0; i < totalSize; i++) {
      result.add(readString(buffer));
    }
    return result;
  }

  public boolean isAuthenticationRequired() {
    return true;
  }

  /** Used to check whether a user has the permission to execute the plan with these paths. */
  public List<? extends PartialPath> getAuthPaths() {
    return getPaths();
  }

  /** If you want to add new PhysicalPlanType, you must add it in the last. */
  public enum PhysicalPlanType {
    INSERT,
    DELETE,
    BATCHINSERT,
    SET_STORAGE_GROUP,
    CREATE_TIMESERIES,
    TTL,
    GRANT_WATERMARK_EMBEDDING,
    REVOKE_WATERMARK_EMBEDDING,
    CREATE_ROLE,
    DELETE_ROLE,
    CREATE_USER,
    REVOKE_USER_ROLE,
    REVOKE_ROLE_PRIVILEGE,
    REVOKE_USER_PRIVILEGE,
    GRANT_ROLE_PRIVILEGE,
    GRANT_USER_PRIVILEGE,
    GRANT_USER_ROLE,
    MODIFY_PASSWORD,
    DELETE_USER,
    DELETE_STORAGE_GROUP,
    SHOW_TIMESERIES,
    DELETE_TIMESERIES,
    LOAD_CONFIGURATION,
    CREATE_MULTI_TIMESERIES,
    ALTER_TIMESERIES,
    FLUSH,
    CREATE_INDEX,
    DROP_INDEX,
    CHANGE_TAG_OFFSET,
    CHANGE_ALIAS,
    MNODE,
    MEASUREMENT_MNODE,
    STORAGE_GROUP_MNODE,
    BATCH_INSERT_ONE_DEVICE,
    MULTI_BATCH_INSERT,
    BATCH_INSERT_ROWS,
    SHOW_DEVICES,
    CREATE_TEMPLATE,
    SET_TEMPLATE,
    ACTIVATE_TEMPLATE,
    AUTO_CREATE_DEVICE_MNODE,
    CREATE_ALIGNED_TIMESERIES,
    CLUSTER_LOG,
    CREATE_TRIGGER,
    DROP_TRIGGER,
    START_TRIGGER,
    STOP_TRIGGER,
    CREATE_CONTINUOUS_QUERY,
    DROP_CONTINUOUS_QUERY,
    SHOW_CONTINUOUS_QUERIES,
    MERGE,
    CREATE_SNAPSHOT, // the snapshot feature has been deprecated, this is kept for compatibility
    CLEARCACHE,
    CREATE_FUNCTION,
    DROP_FUNCTION,
    SELECT_INTO,
    SET_SYSTEM_MODE,
    UNSET_TEMPLATE,
    APPEND_TEMPLATE,
    PRUNE_TEMPLATE,
    START_PIPE_SERVER,
    STOP_PIPE_SERVER,
    DROP_TEMPLATE,
    ACTIVATE_TEMPLATE_IN_CLUSTER,
    PRE_DELETE_TIMESERIES_IN_CLUSTER,
    ROLLBACK_PRE_DELETE_TIMESERIES
  }

  public long getIndex() {
    return index;
  }

  public void setIndex(long index) {
    this.index = index;
  }

  /**
   * Check the integrity of the plan in case that the plan is generated by a careless user through
   * Session API.
   *
   * @throws QueryProcessException when the check fails
   */
  // TODO(INSERT) move this check into analyze
  public void checkIntegrity() throws QueryProcessException {}

  public boolean isPrefixMatch() {
    return isPrefixMatch;
  }

  public void setPrefixMatch(boolean prefixMatch) {
    isPrefixMatch = prefixMatch;
  }
}
