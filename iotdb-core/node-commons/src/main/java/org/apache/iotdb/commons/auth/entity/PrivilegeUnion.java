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

public class PrivilegeUnion {
  private String dbname;
  private String tbname;
  private PartialPath path;

  private boolean grantOption;
  private boolean forAny;
  private final PrivilegeType privilegeType;

  private final PrivilegeModelType modelType;

  public PrivilegeUnion(String dbname, PrivilegeType type, boolean grantOption) {
    this.dbname = dbname;
    this.privilegeType = type;
    this.modelType = PrivilegeModelType.RELATIONAL;
    this.grantOption = grantOption;
  }

  public PrivilegeUnion(String dbname, PrivilegeType type) {
    this.dbname = dbname;
    this.privilegeType = type;
    this.modelType = PrivilegeModelType.RELATIONAL;
  }

  public PrivilegeUnion(String dbname, String tbname, PrivilegeType type, boolean grantOption) {
    this.dbname = dbname;
    this.tbname = tbname;
    this.privilegeType = type;
    this.modelType = PrivilegeModelType.RELATIONAL;
    this.grantOption = grantOption;
  }

  public PrivilegeUnion(String dbname, String tbname, PrivilegeType type) {
    this.dbname = dbname;
    this.tbname = tbname;
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

  public PrivilegeUnion(PrivilegeType type, boolean grantOption, boolean forAny) {
    this.privilegeType = type;
    this.modelType = PrivilegeModelType.RELATIONAL;
    this.grantOption = grantOption;
    this.forAny = forAny;
  }

  public PrivilegeUnion(PrivilegeType type) {
    this.privilegeType = type;
    this.modelType = PrivilegeModelType.SYSTEM;
    this.grantOption = false;
  }

  public PrivilegeModelType modelType() {
    return this.modelType;
  }

  public String getDBName() {
    return this.dbname;
  }

  public String getTbName() {
    return this.tbname;
  }

  public PartialPath getPath() {
    return this.path;
  }

  public PrivilegeType getPrivilegeType() {
    return this.privilegeType;
  }

  public boolean isGrantOption() {
    return this.grantOption;
  }

  public boolean isForAny() {
    return this.forAny;
  }
}
