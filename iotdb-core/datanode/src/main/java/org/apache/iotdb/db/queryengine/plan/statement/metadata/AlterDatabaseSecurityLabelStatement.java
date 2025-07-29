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

package org.apache.iotdb.db.queryengine.plan.statement.metadata;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.SecurityLabel;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.queryengine.plan.analyze.QueryType;
import org.apache.iotdb.db.queryengine.plan.statement.IConfigStatement;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.StatementType;
import org.apache.iotdb.db.queryengine.plan.statement.StatementVisitor;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

/**
 * AlterDatabaseSecurityLabelStatement represents the SQL statement: ALTER DATABASE <dbName> SET
 * SECURITY_LABEL (<key>=<value> [, <key>=<value> ...])
 *
 * <p>This statement is used to set or update security labels for a database to support Label-Based
 * Access Control (LBAC) functionality.
 */
public class AlterDatabaseSecurityLabelStatement extends Statement implements IConfigStatement {

  /** The database name (path) to alter */
  private PartialPath databasePath;

  /** The security labels to set */
  private SecurityLabel securityLabel;

  public AlterDatabaseSecurityLabelStatement() {
    super();
    this.statementType = StatementType.ALTER_DATABASE_SECURITY_LABEL;
  }

  public AlterDatabaseSecurityLabelStatement(
      PartialPath databasePath, SecurityLabel securityLabel) {
    this();
    this.databasePath = databasePath;
    this.securityLabel = securityLabel;
  }

  public PartialPath getDatabasePath() {
    return databasePath;
  }

  public void setDatabasePath(PartialPath databasePath) {
    this.databasePath = databasePath;
  }

  public void setDatabaseName(String databaseName) {
    try {
      this.databasePath = new PartialPath(databaseName);
    } catch (Exception e) {
      throw new IllegalArgumentException("Invalid database name: " + databaseName, e);
    }
  }

  public SecurityLabel getSecurityLabel() {
    return securityLabel;
  }

  public void setSecurityLabel(SecurityLabel securityLabel) {
    this.securityLabel = securityLabel;
  }

  @Override
  public List<PartialPath> getPaths() {
    return databasePath != null ? Collections.singletonList(databasePath) : Collections.emptyList();
  }

  @Override
  public QueryType getQueryType() {
    return QueryType.WRITE;
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitAlterDatabaseSecurityLabel(this, context);
  }

  @Override
  public TSStatus checkPermissionBeforeProcess(String userName) {

    if (AuthorityChecker.SUPER_USER.equals(userName)) {
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    }

    if (databasePath != null) {
      List<PartialPath> paths = Collections.singletonList(databasePath);
      return AuthorityChecker.checkPermissionWithLbac(
          userName, paths, PrivilegeType.MANAGE_DATABASE);
    }

    return AuthorityChecker.getTSStatus(
        AuthorityChecker.checkSystemPermission(userName, PrivilegeType.MANAGE_DATABASE),
        PrivilegeType.MANAGE_DATABASE);
  }

  /**
   * Serializes this statement to ByteBuffer for network transmission or persistence.
   *
   * @param byteBuffer The ByteBuffer to write to
   */
  public void serialize(ByteBuffer byteBuffer) {
    ReadWriteIOUtils.write(statementType.ordinal(), byteBuffer);

    if (databasePath != null) {
      ReadWriteIOUtils.write(true, byteBuffer);
      ReadWriteIOUtils.write(databasePath.getFullPath(), byteBuffer);
    } else {
      ReadWriteIOUtils.write(false, byteBuffer);
    }

    if (securityLabel != null) {
      ReadWriteIOUtils.write(true, byteBuffer);
      securityLabel.serialize(byteBuffer);
    } else {
      ReadWriteIOUtils.write(false, byteBuffer);
    }
  }

  /**
   * Serializes this statement to DataOutputStream for network transmission or persistence.
   *
   * @param stream The DataOutputStream to write to
   * @throws IOException if I/O error occurs
   */
  public void serialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(statementType.ordinal(), stream);

    if (databasePath != null) {
      ReadWriteIOUtils.write(true, stream);
      ReadWriteIOUtils.write(databasePath.getFullPath(), stream);
    } else {
      ReadWriteIOUtils.write(false, stream);
    }

    if (securityLabel != null) {
      ReadWriteIOUtils.write(true, stream);
      securityLabel.serialize(stream);
    } else {
      ReadWriteIOUtils.write(false, stream);
    }
  }

  /**
   * Deserializes an AlterDatabaseSecurityLabelStatement from ByteBuffer.
   *
   * @param byteBuffer The ByteBuffer to read from
   * @return The deserialized statement
   */
  public static AlterDatabaseSecurityLabelStatement deserialize(ByteBuffer byteBuffer) {
    AlterDatabaseSecurityLabelStatement statement = new AlterDatabaseSecurityLabelStatement();

    // Skip StatementType since it's already known
    ReadWriteIOUtils.readInt(byteBuffer);

    // Read database path
    if (ReadWriteIOUtils.readBool(byteBuffer)) {
      String pathStr = ReadWriteIOUtils.readString(byteBuffer);
      try {
        statement.databasePath = new PartialPath(pathStr);
      } catch (Exception e) {
        throw new RuntimeException("Failed to deserialize database path: " + pathStr, e);
      }
    }

    // Read security label
    if (ReadWriteIOUtils.readBool(byteBuffer)) {
      statement.securityLabel = SecurityLabel.deserialize(byteBuffer);
    }

    return statement;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("AlterDatabaseSecurityLabelStatement{");
    sb.append("databasePath=").append(databasePath != null ? databasePath.getFullPath() : "null");
    sb.append(", securityLabel=").append(securityLabel);
    sb.append('}');
    return sb.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof AlterDatabaseSecurityLabelStatement)) {
      return false;
    }
    AlterDatabaseSecurityLabelStatement that = (AlterDatabaseSecurityLabelStatement) o;
    return java.util.Objects.equals(databasePath, that.databasePath)
        && java.util.Objects.equals(securityLabel, that.securityLabel);
  }

  @Override
  public int hashCode() {
    return java.util.Objects.hash(databasePath, securityLabel);
  }
}
