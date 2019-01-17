/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.qp.logical;

import org.apache.iotdb.db.qp.constant.SQLConstant;

/**
 * This class is a superclass of all operator.
 */
public abstract class Operator {

  // operator type in int format
  protected int tokenIntType;
  // operator type in String format
  protected String tokenName;

  protected OperatorType operatorType = OperatorType.NULL;

  public Operator(int tokenIntType) {
    this.tokenIntType = tokenIntType;
    this.tokenName = SQLConstant.tokenNames.get(tokenIntType);
  }

  public OperatorType getType() {
    return operatorType;
  }

  public boolean isQuery() {
    return operatorType == OperatorType.QUERY;
  }

  public int getTokenIntType() {
    return tokenIntType;
  }

  public String getTokenName() {
    return tokenName;
  }

  public void setOperatorType(OperatorType operatorType) {
    this.operatorType = operatorType;
  }

  @Override
  public String toString() {
    return tokenName;
  }

  /**
   * If you want to add new OperatorType, you must add it in the last.
   */
  public enum OperatorType {
    SFW, JOIN, UNION, FILTER, GROUPBY, ORDERBY, LIMIT, SELECT, SEQTABLESCAN, HASHTABLESCAN,
    MERGEJOIN, FILEREAD, NULL, TABLESCAN, UPDATE, INSERT, DELETE, BASIC_FUNC, QUERY, MERGEQUERY,
    AGGREGATION, AUTHOR, FROM, FUNC, LOADDATA, METADATA, PROPERTY, INDEX, INDEXQUERY, FILL,
    SET_STORAGE_GROUP, DELETE_TIMESERIES, CREATE_USER, DELETE_USER, MODIFY_PASSWORD,
    GRANT_USER_PRIVILEGE, REVOKE_USER_PRIVILEGE, GRANT_USER_ROLE, REVOKE_USER_ROLE, CREATE_ROLE,
    DELETE_ROLE, GRANT_ROLE_PRIVILEGE, REVOKE_ROLE_PRIVILEGE, LIST_USER, LIST_ROLE,
    LIST_USER_PRIVILEGE, LIST_ROLE_PRIVILEGE, LIST_USER_ROLES, LIST_ROLE_USERS;
  }
}
