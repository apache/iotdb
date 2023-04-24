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
import java.util.Comparator;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class represents a privilege on a specific seriesPath. If the privilege is seriesPath-free,
 * the seriesPath will be null.I
 */
public class PathPrivilege {
  private static final Logger logger = LoggerFactory.getLogger(PathPrivilege.class);
  private Set<Integer> privileges;
  private PartialPath path;

  /**
   * This field records how many times this privilege is referenced during a life cycle (from being
   * loaded to being discarded). When serialized to a file, this determines the order of
   * serialization. The higher this values is, the sooner this privilege will be serialized. As a
   * result, the hot privileges will be in the first place so that the hit time will decrease when
   * being queried.
   */
  private AtomicInteger referenceCnt = new AtomicInteger(0);

  /** Sort PathPrivilege by referenceCnt in descent order. */
  public static final Comparator<PathPrivilege> REFERENCE_DESCENT_SORTER =
      (o1, o2) -> -Integer.compare(o1.referenceCnt.get(), o2.referenceCnt.get());

  public PathPrivilege() {
    // Empty constructor
  }

  public PathPrivilege(PartialPath path) {
    this.path = path;
    this.privileges = new HashSet<>();
  }

  public Set<Integer> getPrivileges() {
    return privileges;
  }

  public void setPrivileges(Set<Integer> privileges) {
    this.privileges = privileges;
  }

  public PartialPath getPath() {
    return path;
  }

  public void setPath(PartialPath path) {
    this.path = path;
  }

  public AtomicInteger getReferenceCnt() {
    return referenceCnt;
  }

  public void setReferenceCnt(AtomicInteger referenceCnt) {
    this.referenceCnt = referenceCnt;
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
    return Objects.equals(privileges, that.privileges) && Objects.equals(path, that.path);
  }

  @Override
  public int hashCode() {
    return Objects.hash(privileges, path);
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder(path.getFullPath());
    builder.append(" :");
    for (Integer privilegeId : privileges) {
      builder.append(" ").append(PrivilegeType.values()[privilegeId]);
    }
    return builder.toString();
  }

  public ByteBuffer serialize() {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);

    SerializeUtils.serializeIntSet(privileges, dataOutputStream);
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
    path = (PartialPath) PathDeserializeUtil.deserialize(buffer);
  }
}
