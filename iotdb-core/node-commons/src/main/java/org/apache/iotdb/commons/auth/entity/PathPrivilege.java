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
import org.apache.iotdb.commons.utils.SerializeUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/** This class represents a privilege on a specific seriesPath. */
public class PathPrivilege {
  private static final Logger logger = LoggerFactory.getLogger(PathPrivilege.class);

  // privilege capacity: read_data, write_data, read_schema, write_schema;
  private static final int PATH_PRI_SIZE = PrivilegeType.getPathPriCount();
  private Set<Integer> privileges;

  // grantopt show whether the privileges can be grant to / revoke from others.
  // The privilege that can be grant to others must exist in privileges.
  // The set of grantopt must be a subset of privileges.
  private Set<Integer> grantOpts;
  private PartialPath path;

  public PathPrivilege() {
    // Empty constructor
  }

  public PathPrivilege(PartialPath path) {
    this.path = path;
    this.privileges = new HashSet<>();
    this.grantOpts = new HashSet<>();
  }

  public Set<Integer> getPrivileges() {
    return privileges;
  }

  public void setPrivileges(Set<Integer> privileges) {
    this.privileges = privileges;
  }

  public Set<Integer> getGrantOpt() {
    return grantOpts;
  }

  public void setGrantOpt(Set<Integer> grantOpts) {
    this.grantOpts = grantOpts;
  }

  public boolean grantPrivilege(int privilege, boolean grantOpt) {
    privileges.add(privilege);
    if (grantOpt) {
      grantOpts.add(privilege);
    }
    return true;
  }

  public boolean revokePrivilege(int privilege) {
    if (!privileges.contains(privilege)) {
      logger.warn("not find privilege{} on path {}", PrivilegeType.values()[privilege], path);
      return false;
    }
    privileges.remove(privilege);
    // when we revoke privilege from path, remove its grant option
    grantOpts.remove(privilege);
    return true;
  }

  public boolean revokeGrantOpt(int privilege) {
    if (!privileges.contains(privilege)) {
      logger.warn("path {} dont have privilege {}", path, PrivilegeType.values()[privilege]);
      return false;
    }
    grantOpts.remove(privilege);
    return true;
  }

  private int posToPri(int pos) {
    switch (pos) {
      case 0:
        return PrivilegeType.READ_DATA.ordinal();
      case 1:
        return PrivilegeType.WRITE_DATA.ordinal();
      case 2:
        return PrivilegeType.READ_SCHEMA.ordinal();
      case 3:
        return PrivilegeType.WRITE_SCHEMA.ordinal();
      default:
        return -1;
    }
  }

  private int priToPos(PrivilegeType pri) {
    switch (pri) {
      case READ_DATA:
        return 0;
      case WRITE_DATA:
        return 1;
      case READ_SCHEMA:
        return 2;
      case WRITE_SCHEMA:
        return 3;
      default:
        return -1;
    }
  }

  public void setAllPrivileges(int privs) {
    for (int i = 0; i < PATH_PRI_SIZE; i++) {
      if (((1 << i) & privs) != 0) {
        privileges.add(posToPri(i));
      }
      if (((1 << (i + 16) & privs) != 0)) {
        grantOpts.add(posToPri(i));
      }
    }
  }

  public int getAllPrivileges() {
    int privilege = 0;
    for (Integer pri : privileges) {
      privilege |= 1 << priToPos(PrivilegeType.values()[pri]);
    }
    for (Integer grantOpt : grantOpts) {
      privilege |= 1 << (priToPos(PrivilegeType.values()[grantOpt]) + 16);
    }
    return privilege;
  }

  public PartialPath getPath() {
    return path;
  }

  public void setPath(PartialPath path) {
    this.path = path;
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
        && Objects.equals(grantOpts, this.grantOpts);
  }

  @Override
  public int hashCode() {
    return Objects.hash(privileges, grantOpts, path);
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder(path.getFullPath());
    builder.append(" :");
    for (Integer privilegeId : privileges) {
      builder.append(" ").append(PrivilegeType.values()[privilegeId]);
      if (grantOpts.contains(privilegeId)) {
        builder.append("_").append("with_grant_option");
      }
    }
    return builder.toString();
  }

  public ByteBuffer serialize() {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);

    SerializeUtils.serializeIntSet(privileges, dataOutputStream);
    SerializeUtils.serializeIntSet(grantOpts, dataOutputStream);
    try {
      path.serialize(dataOutputStream);
    } catch (IOException exception) {
      logger.error("Unexpected exception when serialize path", exception);
    }
    return ByteBuffer.wrap(byteArrayOutputStream.toByteArray());
  }

  public void deserialize(ByteBuffer buffer) {
    privileges = new HashSet<>();
    SerializeUtils.deserializeIntSet(privileges, buffer);
    grantOpts = new HashSet<>();
    SerializeUtils.deserializeIntSet(grantOpts, buffer);
    path = (PartialPath) PathDeserializeUtil.deserialize(buffer);
  }
}
