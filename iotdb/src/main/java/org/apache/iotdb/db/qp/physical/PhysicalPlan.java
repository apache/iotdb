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
package org.apache.iotdb.db.qp.physical;

import java.util.List;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.tsfile.read.common.Path;

/**
 * This class is a abstract class for all type of PhysicalPlan.
 */
public abstract class PhysicalPlan {

  private boolean isQuery;
  private Operator.OperatorType operatorType;

  /**
   * The name of the user who proposed this operation.
   */
  private String proposer;

  protected PhysicalPlan(boolean isQuery) {
    this.isQuery = isQuery;
  }

  protected PhysicalPlan(boolean isQuery, Operator.OperatorType operatorType) {
    this.isQuery = isQuery;
    this.operatorType = operatorType;
  }

  public String printQueryPlan() {
    return "abstract plan";
  }

  public abstract List<Path> getPaths();

  public boolean isQuery() {
    return isQuery;
  }

  public Operator.OperatorType getOperatorType() {
    return operatorType;
  }

  public void setOperatorType(Operator.OperatorType operatorType) {
    this.operatorType = operatorType;
  }

  public List<String> getAggregations() {
    return null;
  }

  public String getProposer() {
    return proposer;
  }

  public void setProposer(String proposer) {
    this.proposer = proposer;
  }
}
