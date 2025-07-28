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

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathDeserializeUtil;
import org.apache.iotdb.commons.utils.AuthUtils;
import org.apache.iotdb.commons.utils.SerializeUtils;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** This class represents a privilege on a specific seriesPath. */
public class PathPrivilege {

  private PartialPath path;
  private Set<PrivilegeType> privileges;
  private Set<PrivilegeType> grantOpts;

  private static final int PRI_SIZE = PrivilegeType.getPrivilegeCount(PrivilegeModelType.TREE);

  public PathPrivilege() {
    // Empty constructor
  }

  public PathPrivilege(PartialPath path) {
    this.path = path;
    this.privileges = new HashSet<>();
    this.grantOpts = new HashSet<>();
  }

  /** -------- set -------- * */
  public void setPrivileges(Set<PrivilegeType> privs) {
    this.privileges = privs;
  }

  public void setPrivilegesInt(Set<Integer> privs) {
    this.privileges = new HashSet<>();
    for (Integer priv : privs) {
      this.privileges.add(PrivilegeType.values()[priv]);
    }
  }

  public void setGrantOpt(Set<PrivilegeType> grantOpts) {
    this.grantOpts = grantOpts;
  }

  public void setGrantOptInt(Set<Integer> grantOpts) {
    this.grantOpts = new HashSet<>();
    for (Integer priv : grantOpts) {
      this.grantOpts.add(PrivilegeType.values()[priv]);
    }
  }

  public void setPath(PartialPath path) {
    this.path = path;
  }

  public void setAllPrivileges(int privs) {
    for (int i = 0; i < PRI_SIZE; i++) {
      if (((1 << i) & privs) != 0) {
        privileges.add(PrivilegeType.values()[AuthUtils.pathPosToPri(i)]);
      }
      if ((1 << (i + 16) & privs) != 0) {
        grantOpts.add(PrivilegeType.values()[AuthUtils.pathPosToPri(i)]);
      }
    }
  }

  public void grantPrivilege(PrivilegeType privilege, boolean grantOpt) {
    privileges.add(privilege);
    if (grantOpt) {
      grantOpts.add(privilege);
    }
  }

  public boolean revokePrivilege(PrivilegeType privilege) {
    if (!privileges.contains(privilege)) {
      return false;
    }
    privileges.remove(privilege);
    grantOpts.remove(privilege);
    return true;
  }

  public boolean revokeGrantOpt(PrivilegeType privilege) {
    if (!privileges.contains(privilege)) {
      return false;
    }
    grantOpts.remove(privilege);
    return true;
  }

  /** -------- get -------- * */
  public Set<PrivilegeType> getGrantOpt() {
    return grantOpts;
  }

  public Set<Integer> getGrantOptIntSet() {
    Set<Integer> res = new HashSet<>();
    for (PrivilegeType item : grantOpts) {
      res.add(item.ordinal());
    }
    return res;
  }

  public Set<Integer> getPrivilegeIntSet() {
    Set<Integer> res = new HashSet<>();
    for (PrivilegeType item : privileges) {
      res.add(item.ordinal());
    }
    return res;
  }

  public Set<PrivilegeType> getPrivileges() {
    return privileges;
  }

  public boolean checkPrivilege(PrivilegeType privilege) {
    if (privileges.contains(privilege)) {
      return true;
    }

    if (privilege == PrivilegeType.READ_DATA) {
      return privileges.contains(PrivilegeType.WRITE_DATA);
    }

    if (privilege == PrivilegeType.READ_SCHEMA) {
      return privileges.contains(PrivilegeType.WRITE_SCHEMA);
    }
    return false;
  }

  public int getAllPrivileges() {
    int privilege = 0;
    for (PrivilegeType pri : privileges) {
      privilege |= 1 << AuthUtils.pathPriToPos(pri);
    }
    for (PrivilegeType grantOpt : grantOpts) {
      privilege |= 1 << (AuthUtils.pathPriToPos(grantOpt) + 16);
    }
    return privilege;
  }

  public PartialPath getPath() {
    return path;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PathPrivilege that = (PathPrivilege) o;
    return Objects.equals(privileges, that.privileges)
        && Objects.equals(path, that.path)
        && Objects.equals(grantOpts, that.grantOpts);
  }

  @Override
  public int hashCode() {
    return Objects.hash(privileges, grantOpts, path);
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder(path.getFullPath());
    builder.append(" :");
    List<PrivilegeType> sortedPrivileges = new ArrayList<>(privileges);
    Collections.sort(sortedPrivileges);
    for (PrivilegeType privilege : sortedPrivileges) {
      builder.append(" ").append(privilege);
      if (grantOpts.contains(privilege)) {
        builder.append("_").append("with_grant_option");
      }
    }
    return builder.toString();
  }

  public ByteBuffer serialize() throws IOException {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);

    SerializeUtils.serializePrivilegeTypeSet(privileges, dataOutputStream);
    SerializeUtils.serializePrivilegeTypeSet(grantOpts, dataOutputStream);
    path.serialize(dataOutputStream);
    return ByteBuffer.wrap(byteArrayOutputStream.toByteArray());
  }

  public void deserialize(ByteBuffer buffer) {
    privileges = new HashSet<>();
    SerializeUtils.deserializePrivilegeTypeSet(privileges, buffer);
    grantOpts = new HashSet<>();
    SerializeUtils.deserializePrivilegeTypeSet(grantOpts, buffer);
    path = (PartialPath) PathDeserializeUtil.deserialize(buffer);
  }
}
