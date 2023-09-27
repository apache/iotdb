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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public enum PriPrivilegeType {
  READ_DATA(true, PrivilegeType.READ_DATA),
  WRITE_DATA(true, PrivilegeType.WRITE_DATA),
  READ_SCHEMA(true, PrivilegeType.READ_SCHEMA),
  WRITE_SCHEMA(true, PrivilegeType.WRITE_SCHEMA),
  MANAGE_USER(false, PrivilegeType.MANAGE_USER),
  MANAGE_ROLE(false, PrivilegeType.MANAGE_ROLE),
  GRANT_PRIVILEGE(false),
  ALTER_PASSWORD(false),
  USE_TRIGGER(false, PrivilegeType.USE_TRIGGER),
  USE_CQ(false, PrivilegeType.USE_CQ),
  USE_PIPE(false, PrivilegeType.USE_PIPE),
  MANAGE_DATABASE(false, PrivilegeType.MANAGE_DATABASE),
  MAINTAIN(false, PrivilegeType.MAINTAIN),
  READ(true, PrivilegeType.READ_DATA, PrivilegeType.READ_SCHEMA),
  WRITE(true, PrivilegeType.WRITE_DATA, PrivilegeType.WRITE_SCHEMA),
  ALL(
      true,
      PrivilegeType.READ_SCHEMA,
      PrivilegeType.READ_DATA,
      PrivilegeType.WRITE_DATA,
      PrivilegeType.WRITE_SCHEMA,
      PrivilegeType.MANAGE_USER,
      PrivilegeType.MANAGE_ROLE,
      PrivilegeType.USE_TRIGGER,
      PrivilegeType.USE_CQ,
      PrivilegeType.USE_PIPE,
      PrivilegeType.USE_UDF,
      PrivilegeType.MANAGE_DATABASE,
      PrivilegeType.MAINTAIN,
      PrivilegeType.EXTEND_TEMPLATE,
      PrivilegeType.AUDIT);

  boolean accept = false;
  private final boolean isPathRelevant;
  private final List<PrivilegeType> refPri = new ArrayList<>();

  PriPrivilegeType(boolean accept) {
    this.accept = accept;
    this.isPathRelevant = false;
  }

  PriPrivilegeType(boolean isPathRelevant, PrivilegeType... privilegeTypes) {
    this.accept = true;
    this.isPathRelevant = isPathRelevant;
    this.refPri.addAll(Arrays.asList(privilegeTypes));
  }

  public boolean isAccept() {
    return this.accept;
  }

  public boolean isPathRelevant() {
    return this.isPathRelevant;
  }

  public Set<PrivilegeType> getSubPri() {
    Set<PrivilegeType> result = new HashSet<>();
    for (PrivilegeType peivType : refPri) {
      result.add(peivType);
    }
    return result;
  }
}
