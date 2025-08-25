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

package org.apache.iotdb.confignode.procedure.impl.schema;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.SecurityLabel;
import org.apache.iotdb.confignode.consensus.request.write.database.SetDatabaseSecurityLabelPlan;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.impl.StateMachineProcedure;
import org.apache.iotdb.confignode.procedure.state.schema.AlterDatabaseSecurityLabelState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * Simplified AlterDatabaseSecurityLabelTask for setting security labels on databases.
 *
 * <p>This is a basic implementation that handles the procedure of setting security labels for
 * databases in a distributed IoTDB cluster without complex error handling.
 */
public class AlterDatabaseSecurityLabelTask
    extends StateMachineProcedure<ConfigNodeProcedureEnv, AlterDatabaseSecurityLabelState> {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(AlterDatabaseSecurityLabelTask.class);

  /** The database path to alter */
  private PartialPath databasePath;

  /** The security labels to set */
  private SecurityLabel securityLabel;

  /** Default constructor for deserialization */
  public AlterDatabaseSecurityLabelTask() {
    super();
  }

  /**
   * Constructor with parameters.
   *
   * @param databasePath the path of the database
   * @param securityLabel the security label to set
   */
  public AlterDatabaseSecurityLabelTask(PartialPath databasePath, SecurityLabel securityLabel) {
    super();
    this.databasePath = databasePath;
    this.securityLabel = securityLabel;
  }

  @Override
  protected Flow executeFromState(ConfigNodeProcedureEnv env, AlterDatabaseSecurityLabelState state)
      throws InterruptedException {
    long startTime = System.currentTimeMillis();
    LOGGER.info("Executing AlterDatabaseSecurityLabelTask state: {}", state);

    try {
      switch (state) {
        case VALIDATE_DATABASE:
          LOGGER.info("Validating database existence: {}", databasePath);
          validateDatabase(env);
          setNextState(AlterDatabaseSecurityLabelState.SET_SECURITY_LABEL);
          break;

        case SET_SECURITY_LABEL:
          LOGGER.info("Setting security label for database: {}", databasePath);
          setSecurityLabel(env);
          setNextState(AlterDatabaseSecurityLabelState.FINISH);
          break;

        case FINISH:
          LOGGER.info("Successfully set security label for database: {}", databasePath);
          return Flow.NO_MORE_STATE;

        default:
          LOGGER.error("Unsupported state: {}", state);
          setFailure("Unsupported state: " + state, new IllegalStateException());
          return Flow.NO_MORE_STATE;
      }
    } catch (Exception e) {
      LOGGER.error("Failed to execute state {}: {}", state, e.getMessage(), e);
      setFailure("Failed to set security label", e);
      return Flow.NO_MORE_STATE;
    } finally {
      LOGGER.debug(
          "AlterDatabaseSecurityLabelTask-{} completed in {}ms",
          state,
          System.currentTimeMillis() - startTime);
    }

    return Flow.HAS_MORE_STATE;
  }

  /** Validates that the database exists */
  private void validateDatabase(ConfigNodeProcedureEnv env) throws Exception {
    // Check if the database exists in the cluster schema
    boolean exists =
        env.getConfigManager()
            .getClusterSchemaManager()
            .isDatabaseExist(databasePath.getFullPath());

    if (!exists) {
      throw new Exception("Database [" + databasePath.getFullPath() + "] does not exist");
    }
    LOGGER.info("Database {} exists, validation passed", databasePath.getFullPath());
  }

  /** Sets the security label for the database */
  private void setSecurityLabel(ConfigNodeProcedureEnv env) throws Exception {
    SetDatabaseSecurityLabelPlan plan =
        new SetDatabaseSecurityLabelPlan(databasePath, securityLabel);

    TSStatus status = env.getConfigManager().getConsensusManager().write(plan);

    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new Exception(
          "Failed to set security label for database ["
              + databasePath.getFullPath()
              + "]: "
              + status.getMessage());
    }

    LOGGER.info("Successfully set security label for database {}", databasePath.getFullPath());
  }

  @Override
  protected void rollbackState(ConfigNodeProcedureEnv env, AlterDatabaseSecurityLabelState state)
      throws IOException, InterruptedException {
    switch (state) {
      case SET_SECURITY_LABEL:
        LOGGER.info("Rolling back security label setting for database: {}", databasePath);
        // Note: In a real implementation, you might want to restore the previous labels
        // For now, we just log the rollback
        break;
      default:
        LOGGER.debug("No rollback needed for state: {}", state);
        break;
    }
  }

  @Override
  protected boolean isRollbackSupported(AlterDatabaseSecurityLabelState state) {
    switch (state) {
      case SET_SECURITY_LABEL:
        return true;
      default:
        return false;
    }
  }

  @Override
  protected AlterDatabaseSecurityLabelState getState(int stateId) {
    return AlterDatabaseSecurityLabelState.values()[stateId];
  }

  @Override
  protected int getStateId(AlterDatabaseSecurityLabelState state) {
    return state.ordinal();
  }

  @Override
  protected AlterDatabaseSecurityLabelState getInitialState() {
    return AlterDatabaseSecurityLabelState.VALIDATE_DATABASE;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeShort(ProcedureType.ALTER_DATABASE_SECURITY_LABEL_PROCEDURE.getTypeCode());
    super.serialize(stream);

    // Serialize database path
    if (databasePath != null) {
      ReadWriteIOUtils.write(true, stream);
      ReadWriteIOUtils.write(databasePath.getFullPath(), stream);
    } else {
      ReadWriteIOUtils.write(false, stream);
    }

    // Serialize security label
    if (securityLabel != null) {
      ReadWriteIOUtils.write(true, stream);
      securityLabel.serialize(stream);
    } else {
      ReadWriteIOUtils.write(false, stream);
    }
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);

    // Deserialize database path
    if (ReadWriteIOUtils.readBool(byteBuffer)) {
      String pathStr = ReadWriteIOUtils.readString(byteBuffer);
      try {
        this.databasePath = new PartialPath(pathStr);
      } catch (Exception e) {
        LOGGER.error("Failed to deserialize database path: {}", pathStr, e);
        throw new RuntimeException("Failed to deserialize database path: " + pathStr, e);
      }
    }

    // Deserialize security label
    if (ReadWriteIOUtils.readBool(byteBuffer)) {
      this.securityLabel = SecurityLabel.deserialize(byteBuffer);
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof AlterDatabaseSecurityLabelTask)) {
      return false;
    }
    AlterDatabaseSecurityLabelTask that = (AlterDatabaseSecurityLabelTask) obj;
    return Objects.equals(this.databasePath, that.databasePath)
        && Objects.equals(this.securityLabel, that.securityLabel);
  }

  @Override
  public int hashCode() {
    return Objects.hash(databasePath, securityLabel);
  }

  // Getter methods
  public PartialPath getDatabasePath() {
    return databasePath;
  }

  public SecurityLabel getSecurityLabel() {
    return securityLabel;
  }

  @Override
  public String toString() {
    return String.format(
        "AlterDatabaseSecurityLabelTask{databasePath=%s, securityLabel=%s}",
        databasePath != null ? databasePath.getFullPath() : "null", securityLabel);
  }
}
