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
package org.apache.iotdb.commons.auth.entity;

import org.apache.iotdb.commons.utils.AuthUtils;
import org.apache.iotdb.commons.utils.SerializeUtils;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

// This class contain table's privileges.
public class TablePrivilege {
  private String tableName;
  private Set<PrivilegeType> privileges;
  private Set<PrivilegeType> grantOption;

  public TablePrivilege(String tableName) {
    this.tableName = tableName;
    this.privileges = new HashSet<>();
    this.grantOption = new HashSet<>();
  }

  public TablePrivilege() {
    // this construction just used for deserialize.
  }

  public String getTableName() {
    return this.tableName;
  }

  public Set<PrivilegeType> getPrivileges() {
    return this.privileges;
  }

  public Set<PrivilegeType> getGrantOption() {
    return this.grantOption;
  }

  public Set<Integer> getPrivilegesIntSet() {
    Set<Integer> res = new HashSet<>();
    for (PrivilegeType type : privileges) {
      res.add(type.ordinal());
    }
    return res;
  }

  public Set<Integer> getGrantOptionIntSet() {
    Set<Integer> res = new HashSet<>();
    for (PrivilegeType type : grantOption) {
      res.add(type.ordinal());
    }
    return res;
  }

  public void grantPrivilege(PrivilegeType priv) {
    this.privileges.add(priv);
  }

  public void revokePrivilege(PrivilegeType priv) {
    this.privileges.remove(priv);
  }

  public void grantOption(PrivilegeType priv) {
    this.grantOption.add(priv);
  }

  public void revokeGrantOption(PrivilegeType priv) {
    this.grantOption.remove(priv);
  }

  public void setPrivileges(int mask) {
    final int PRI_SIZE = PrivilegeType.getPrivilegeCount(PrivilegeModelType.RELATIONAL);
    for (int i = 0; i < PRI_SIZE; i++) {
      if (((1 << i) & mask) != 0) {
        this.privileges.add(AuthUtils.posToObjPri(i));
        if (((1 << (i + 16)) & mask) != 0) {
          this.grantOption.add(AuthUtils.posToObjPri(i));
        }
      }
    }
  }

  public int getAllPrivileges() {
    int privilege = 0;
    for (PrivilegeType pri : privileges) {
      privilege |= 1 << AuthUtils.objPriToPos(pri);
    }
    for (PrivilegeType pri : grantOption) {
      privilege |= 1 << (AuthUtils.objPriToPos(pri) + 16);
    }
    return privilege;
  }

  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TablePrivilege that = (TablePrivilege) o;
    return tableName.equals(that.tableName)
        && privileges.equals(that.privileges)
        && grantOption.equals(that.grantOption);
  }

  public int hashCode() {
    return Objects.hash(tableName, privileges, grantOption);
  }

  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append(this.tableName).append("(");
    List<PrivilegeType> privs = new ArrayList<>(this.privileges);
    Collections.sort(privs);
    for (PrivilegeType type : privs) {
      builder.append(type);
      if (grantOption.contains(type)) {
        builder.append("_with_grant_option");
      }
      builder.append(",");
    }
    builder.append(")");
    return builder.toString();
  }

  public void serialize(OutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(this.tableName, outputStream);
    SerializeUtils.serializePrivilegeTypeSet(this.privileges, (DataOutputStream) outputStream);
    SerializeUtils.serializePrivilegeTypeSet(this.grantOption, (DataOutputStream) outputStream);
  }

  public void deserialize(ByteBuffer byteBuffer) {
    this.privileges = new HashSet<>();
    this.grantOption = new HashSet<>();
    this.tableName = SerializeUtils.deserializeString(byteBuffer);
    SerializeUtils.deserializePrivilegeTypeSet(this.privileges, byteBuffer);
    SerializeUtils.deserializePrivilegeTypeSet(this.grantOption, byteBuffer);
  }
}
