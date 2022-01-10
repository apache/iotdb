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

public class CreatePipeSinkPlan extends PhysicalPlan {
  private String pipeSinkName;
  private String pipeSinkType;
  private List<Pair<String, String>> pipeSinkAttributes;

  public CreatePipeSinkPlan(String pipeSinkName, String pipeSinkType) {
    super(Operator.OperatorType.CREATE_PIPESINK);
    this.pipeSinkName = pipeSinkName;
    this.pipeSinkType = pipeSinkType;
    pipeSinkAttributes = new ArrayList<>();
  }

  public void addPipeSinkAttribute(String attr, String value) {
    pipeSinkAttributes.add(new Pair<>(attr, value));
  }

  public String getPipeSinkName() {
    return pipeSinkName;
  }

  public String getPipeSinkType() {
    return pipeSinkType;
  }

  public List<Pair<String, String>> getPipeSinkAttributes() {
    return pipeSinkAttributes;
  }

  @Override
  public List<? extends PartialPath> getPaths() {
    return Collections.emptyList();
  }
}
