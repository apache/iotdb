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
package org.apache.iotdb.db.qp.physical.sys;

import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.tsfile.utils.Pair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class CreatePipePlan extends PhysicalPlan {
  private String pipeName;
  private String pipeSinkName;
  private long startTime;
  private List<Pair<String, String>> pipeAttributes;

  public CreatePipePlan(String pipeName, String pipeSinkName) {
    super(Operator.OperatorType.CREATE_PIPE);
    this.pipeName = pipeName;
    this.pipeSinkName = pipeSinkName;
    startTime = 0;
    pipeAttributes = new ArrayList<>();
  }

  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  public void addPipeAttribute(String attr, String value) {
    pipeAttributes.add(new Pair<>(attr, value));
  }

  @Override
  public List<? extends PartialPath> getPaths() {
    return Collections.emptyList();
  }
}
