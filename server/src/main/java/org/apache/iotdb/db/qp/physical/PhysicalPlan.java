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
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.exception.runtime.SerializationRunTimeException;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.logical.Operator.OperatorType;
import org.apache.iotdb.db.qp.physical.crud.DeletePlan;
import org.apache.iotdb.db.qp.physical.crud.InsertMultiTabletsPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowsOfOneDevicePlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowsPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertTabletPlan;
import org.apache.iotdb.db.qp.physical.crud.SelectIntoPlan;
import org.apache.iotdb.db.qp.physical.sys.ActivateTemplateInClusterPlan;
import org.apache.iotdb.db.qp.physical.sys.ActivateTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.AlterTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.AppendTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.AuthorPlan;
import org.apache.iotdb.db.qp.physical.sys.AutoCreateDeviceMNodePlan;
import org.apache.iotdb.db.qp.physical.sys.ChangeAliasPlan;
import org.apache.iotdb.db.qp.physical.sys.ChangeTagOffsetPlan;
import org.apache.iotdb.db.qp.physical.sys.ClearCachePlan;
import org.apache.iotdb.db.qp.physical.sys.CreateAlignedTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateContinuousQueryPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateFunctionPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateIndexPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateMultiTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTriggerPlan;
import org.apache.iotdb.db.qp.physical.sys.DataAuthPlan;
import org.apache.iotdb.db.qp.physical.sys.DeleteStorageGroupPlan;
import org.apache.iotdb.db.qp.physical.sys.DeleteTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.DropContinuousQueryPlan;
import org.apache.iotdb.db.qp.physical.sys.DropFunctionPlan;
import org.apache.iotdb.db.qp.physical.sys.DropIndexPlan;
import org.apache.iotdb.db.qp.physical.sys.DropTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.DropTriggerPlan;
import org.apache.iotdb.db.qp.physical.sys.FlushPlan;
import org.apache.iotdb.db.qp.physical.sys.LoadConfigurationPlan;
import org.apache.iotdb.db.qp.physical.sys.LogPlan;
import org.apache.iotdb.db.qp.physical.sys.MNodePlan;
import org.apache.iotdb.db.qp.physical.sys.MeasurementMNodePlan;
import org.apache.iotdb.db.qp.physical.sys.MergePlan;
import org.apache.iotdb.db.qp.physical.sys.PruneTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.SetStorageGroupPlan;
import org.apache.iotdb.db.qp.physical.sys.SetSystemModePlan;
import org.apache.iotdb.db.qp.physical.sys.SetTTLPlan;
import org.apache.iotdb.db.qp.physical.sys.SetTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.ShowDevicesPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.StartTriggerPlan;
import org.apache.iotdb.db.qp.physical.sys.StopTriggerPlan;
import org.apache.iotdb.db.qp.physical.sys.StorageGroupMNodePlan;
import org.apache.iotdb.db.qp.physical.sys.UnsetTemplatePlan;
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

  /**
   * Deserialize the plan from the given buffer.
   *
   * @param buffer
   */
  public void deserialize(ByteBuffer buffer) throws IllegalPathException, IOException {
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

  public String getLoginUserName() {
    return loginUserName;
  }

  public void setLoginUserName(String loginUserName) {
    if (this instanceof AuthorPlan) {
      this.loginUserName = loginUserName;
    }
  }

  public boolean isAuthenticationRequired() {
    return true;
  }

  /** Used to check whether a user has the permission to execute the plan with these paths. */
  public List<? extends PartialPath> getAuthPaths() {
    return getPaths();
  }

  public static class Factory {

    private Factory() {
      // hidden initializer
    }

    public static PhysicalPlan create(ByteBuffer buffer) throws IOException, IllegalPathException {
      int typeNum = buffer.get();
      PhysicalPlan plan = createByTypeNum(typeNum);
      plan.deserialize(buffer);
      return plan;
    }

    public static PhysicalPlan create(DataInputStream stream)
        throws IOException, IllegalPathException {
      int typeNum = stream.readByte();
      PhysicalPlan plan = createByTypeNum(typeNum);
      plan.deserialize(stream);
      return plan;
    }

    private static PhysicalPlan createByTypeNum(int typeNum) throws IOException {
      if (typeNum < 0 || typeNum >= PhysicalPlanType.values().length) {
        throw new IOException("unrecognized log type " + typeNum);
      }
      PhysicalPlanType type = PhysicalPlanType.values()[typeNum];
      PhysicalPlan plan;
      // TODO-Cluster: support more plans
      switch (type) {
        case INSERT:
          plan = new InsertRowPlan();
          break;
        case BATCHINSERT:
          plan = new InsertTabletPlan();
          break;
        case MULTI_BATCH_INSERT:
          plan = new InsertMultiTabletsPlan();
          break;
        case DELETE:
          plan = new DeletePlan();
          break;
        case SET_STORAGE_GROUP:
          plan = new SetStorageGroupPlan();
          break;
        case CREATE_TIMESERIES:
          plan = new CreateTimeSeriesPlan();
          break;
        case CREATE_ALIGNED_TIMESERIES:
          plan = new CreateAlignedTimeSeriesPlan();
          break;
        case DELETE_TIMESERIES:
          plan = new DeleteTimeSeriesPlan();
          break;
        case CREATE_INDEX:
          plan = new CreateIndexPlan();
          break;
        case DROP_INDEX:
          plan = new DropIndexPlan();
          break;
        case TTL:
          plan = new SetTTLPlan();
          break;
        case GRANT_WATERMARK_EMBEDDING:
          plan = new DataAuthPlan(OperatorType.GRANT_WATERMARK_EMBEDDING);
          break;
        case REVOKE_WATERMARK_EMBEDDING:
          plan = new DataAuthPlan(OperatorType.REVOKE_WATERMARK_EMBEDDING);
          break;
        case CREATE_ROLE:
          plan = new AuthorPlan(OperatorType.CREATE_ROLE);
          break;
        case DELETE_ROLE:
          plan = new AuthorPlan(OperatorType.DELETE_ROLE);
          break;
        case CREATE_USER:
          plan = new AuthorPlan(OperatorType.CREATE_USER);
          break;
        case REVOKE_USER_ROLE:
          plan = new AuthorPlan(OperatorType.REVOKE_USER_ROLE);
          break;
        case REVOKE_ROLE_PRIVILEGE:
          plan = new AuthorPlan(OperatorType.REVOKE_ROLE_PRIVILEGE);
          break;
        case REVOKE_USER_PRIVILEGE:
          plan = new AuthorPlan(OperatorType.REVOKE_USER_PRIVILEGE);
          break;
        case GRANT_ROLE_PRIVILEGE:
          plan = new AuthorPlan(OperatorType.GRANT_ROLE_PRIVILEGE);
          break;
        case GRANT_USER_PRIVILEGE:
          plan = new AuthorPlan(OperatorType.GRANT_USER_PRIVILEGE);
          break;
        case GRANT_USER_ROLE:
          plan = new AuthorPlan(OperatorType.GRANT_USER_ROLE);
          break;
        case MODIFY_PASSWORD:
          plan = new AuthorPlan(OperatorType.MODIFY_PASSWORD);
          break;
        case DELETE_USER:
          plan = new AuthorPlan(OperatorType.DELETE_USER);
          break;
        case DELETE_STORAGE_GROUP:
          plan = new DeleteStorageGroupPlan();
          break;
        case SHOW_TIMESERIES:
          plan = new ShowTimeSeriesPlan();
          break;
        case SHOW_DEVICES:
          plan = new ShowDevicesPlan();
          break;
        case LOAD_CONFIGURATION:
          plan = new LoadConfigurationPlan();
          break;
        case ALTER_TIMESERIES:
          plan = new AlterTimeSeriesPlan();
          break;
        case FLUSH:
          plan = new FlushPlan();
          break;
        case CREATE_MULTI_TIMESERIES:
          plan = new CreateMultiTimeSeriesPlan();
          break;
        case CHANGE_ALIAS:
          plan = new ChangeAliasPlan();
          break;
        case CHANGE_TAG_OFFSET:
          plan = new ChangeTagOffsetPlan();
          break;
        case MNODE:
          plan = new MNodePlan();
          break;
        case MEASUREMENT_MNODE:
          plan = new MeasurementMNodePlan();
          break;
        case STORAGE_GROUP_MNODE:
          plan = new StorageGroupMNodePlan();
          break;
        case BATCH_INSERT_ROWS:
          plan = new InsertRowsPlan();
          break;
        case BATCH_INSERT_ONE_DEVICE:
          plan = new InsertRowsOfOneDevicePlan();
          break;
        case CREATE_TRIGGER:
          plan = new CreateTriggerPlan();
          break;
        case DROP_TRIGGER:
          plan = new DropTriggerPlan();
          break;
        case START_TRIGGER:
          plan = new StartTriggerPlan();
          break;
        case STOP_TRIGGER:
          plan = new StopTriggerPlan();
          break;
        case CLUSTER_LOG:
          plan = new LogPlan();
          break;
        case CREATE_TEMPLATE:
          plan = new CreateTemplatePlan();
          break;
        case APPEND_TEMPLATE:
          plan = new AppendTemplatePlan();
          break;
        case PRUNE_TEMPLATE:
          plan = new PruneTemplatePlan();
          break;
        case DROP_TEMPLATE:
          plan = new DropTemplatePlan();
          break;
        case UNSET_TEMPLATE:
          plan = new UnsetTemplatePlan();
          break;
        case SET_TEMPLATE:
          plan = new SetTemplatePlan();
          break;
        case ACTIVATE_TEMPLATE:
          plan = new ActivateTemplatePlan();
          break;
        case AUTO_CREATE_DEVICE_MNODE:
          plan = new AutoCreateDeviceMNodePlan();
          break;
        case CREATE_CONTINUOUS_QUERY:
          plan = new CreateContinuousQueryPlan();
          break;
        case DROP_CONTINUOUS_QUERY:
          plan = new DropContinuousQueryPlan();
          break;
        case MERGE:
          plan = new MergePlan();
          break;
        case CLEARCACHE:
          plan = new ClearCachePlan();
          break;
        case CREATE_FUNCTION:
          plan = new CreateFunctionPlan();
          break;
        case DROP_FUNCTION:
          plan = new DropFunctionPlan();
          break;
        case SELECT_INTO:
          plan = new SelectIntoPlan();
          break;
        case SET_SYSTEM_MODE:
          plan = new SetSystemModePlan();
          break;
        case ACTIVATE_TEMPLATE_IN_CLUSTER:
          plan = new ActivateTemplateInClusterPlan();
          break;
        default:
          throw new IOException("unrecognized log type " + type);
      }
      return plan;
    }
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
    ACTIVATE_TEMPLATE_IN_CLUSTER
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
