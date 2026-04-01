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

import java.util.List;

public class PrivilegeUnion {
  private String databaseName;
  private String tableName;
  private PartialPath path;

  private List<? extends PartialPath> paths;

  private boolean grantOption;
  private boolean forAny;
  private final PrivilegeType privilegeType;

  private final PrivilegeModelType modelType;

  public PrivilegeUnion(String databaseName, PrivilegeType type, boolean grantOption) {
    this.databaseName = databaseName;
    this.privilegeType = type;
    this.modelType = PrivilegeModelType.RELATIONAL;
    this.grantOption = grantOption;
  }

  public PrivilegeUnion(String databaseName, PrivilegeType type) {
    this.databaseName = databaseName;
    this.privilegeType = type;
    this.modelType = PrivilegeModelType.RELATIONAL;
  }

  public PrivilegeUnion(
      String databaseName, String tableName, PrivilegeType type, boolean grantOption) {
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.privilegeType = type;
    this.modelType = PrivilegeModelType.RELATIONAL;
    this.grantOption = grantOption;
  }

  public PrivilegeUnion(String databaseName, String tableName, PrivilegeType type) {
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.privilegeType = type;
    this.modelType = PrivilegeModelType.RELATIONAL;
  }

  public PrivilegeUnion(PartialPath path, PrivilegeType type, boolean grantOption) {
    this.path = path;
    this.privilegeType = type;
    this.modelType = PrivilegeModelType.TREE;
    this.grantOption = grantOption;
  }

  public PrivilegeUnion(PartialPath path, PrivilegeType type) {
    this.path = path;
    this.privilegeType = type;
    this.modelType = PrivilegeModelType.TREE;
    this.grantOption = false;
  }

  public PrivilegeUnion(PrivilegeType type, boolean grantOption) {
    this.privilegeType = type;
    this.modelType = PrivilegeModelType.SYSTEM;
    this.grantOption = grantOption;
  }

  public PrivilegeUnion(
      List<? extends PartialPath> paths, PrivilegeType type, boolean grantOption) {
    this.paths = paths;
    this.privilegeType = type;
    this.modelType = PrivilegeModelType.TREE;
    this.grantOption = grantOption;
  }

  public PrivilegeUnion(List<? extends PartialPath> paths, PrivilegeType type) {
    this.paths = paths;
    this.privilegeType = type;
    this.modelType = PrivilegeModelType.TREE;
    this.grantOption = false;
  }

  public PrivilegeUnion(PrivilegeType type, boolean grantOption, boolean forAny) {
    this.privilegeType = type;
    this.modelType = forAny ? PrivilegeModelType.RELATIONAL : PrivilegeModelType.SYSTEM;
    this.grantOption = grantOption;
    this.forAny = forAny;
  }

  public PrivilegeUnion(PrivilegeType type) {
    this.privilegeType = type;
    this.modelType = PrivilegeModelType.SYSTEM;
    this.grantOption = false;
  }

  public void setGrantOption(boolean grantOption) {
    this.grantOption = grantOption;
  }

  public PrivilegeModelType getModelType() {
    return this.modelType;
  }

  public String getDBName() {
    return this.databaseName;
  }

  public String getTbName() {
    return this.tableName;
  }

  public PartialPath getPath() {
    return this.path;
  }

  public PrivilegeType getPrivilegeType() {
    return this.privilegeType;
  }

  public List<? extends PartialPath> getPaths() {
    return this.paths;
  }

  public boolean isGrantOption() {
    return this.grantOption;
  }

  public boolean isForAny() {
    return this.forAny;
  }
}
