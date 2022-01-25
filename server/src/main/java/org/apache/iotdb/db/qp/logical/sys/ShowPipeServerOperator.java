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
 *
 */
package org.apache.iotdb.db.qp.logical.sys;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowPipeServerPlan;
import org.apache.iotdb.db.qp.strategy.PhysicalGenerator;

import org.apache.commons.lang3.StringUtils;

public class ShowPipeServerOperator extends ShowOperator {

  private String pipeName;

  public ShowPipeServerOperator(String pipeName, int tokenIntType) {
    this(tokenIntType);
    this.pipeName = pipeName;
  }

  public ShowPipeServerOperator(int tokenIntType) {
    super(tokenIntType);
  }

  @Override
  public PhysicalPlan generatePhysicalPlan(PhysicalGenerator generator)
      throws QueryProcessException {
    if (StringUtils.isEmpty(pipeName)) {
      return new ShowPipeServerPlan();
    } else {
      return new ShowPipeServerPlan(pipeName);
    }
  }
}
