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
import org.apache.iotdb.db.newsync.sender.conf.SenderConf;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.tsfile.utils.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class CreatePipePlan extends PhysicalPlan {
  private String pipeName;
  private String pipeSinkName;
  private long dataStartTimestamp;
  private List<Pair<String, String>> pipeAttributes;

  public CreatePipePlan(String pipeName, String pipeSinkName) {
    super(Operator.OperatorType.CREATE_PIPE);
    this.pipeName = pipeName;
    this.pipeSinkName = pipeSinkName;
    dataStartTimestamp = 0;
    pipeAttributes = new ArrayList<>();
  }

  public void setDataStartTimestamp(long dataStartTimestamp) {
    this.dataStartTimestamp = dataStartTimestamp;
  }

  public void addPipeAttribute(String attr, String value) {
    pipeAttributes.add(new Pair<>(attr, value));
  }

  public String getPipeName() {
    return pipeName;
  }

  public String getPipeSinkName() {
    return pipeSinkName;
  }

  public long getDataStartTimestamp() {
    return dataStartTimestamp;
  }

  public List<Pair<String, String>> getPipeAttributes() {
    return pipeAttributes;
  }

  @Override
  public List<? extends PartialPath> getPaths() {
    return Collections.emptyList();
  }

  public static CreatePipePlan parseString(String parsedString) throws IOException {
    String[] attributes = parsedString.split(SenderConf.planSplitCharacter);
    if (attributes.length < 4) {
      throw new IOException("Parsing CreatePipePlan error. Attributes is less than expected.");
    }
    CreatePipePlan plan = new CreatePipePlan(attributes[0], attributes[1]);
    plan.setDataStartTimestamp(Long.parseLong(attributes[2]));
    int size = (Integer.parseInt(attributes[3]) << 1);
    if (attributes.length != (size + 4)) {
      throw new IOException("Parsing CreatePipePlan error. Attributes number is wrong.");
    }
    for (int i = 0; i < size; i += 2) {
      plan.addPipeAttribute(attributes[i + 4], attributes[i + 5]);
    }
    return plan;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append(pipeName).append(SenderConf.planSplitCharacter);
    builder.append(pipeSinkName).append(SenderConf.planSplitCharacter);
    builder.append(dataStartTimestamp).append(SenderConf.planSplitCharacter);
    builder.append(pipeAttributes.size()).append(SenderConf.planSplitCharacter);
    for (int i = 0; i < pipeAttributes.size(); i++) {
      builder.append(pipeAttributes.get(i).left).append(SenderConf.planSplitCharacter);
      builder.append(pipeAttributes.get(i).right).append(SenderConf.planSplitCharacter);
    }
    return builder.toString();
  }
}
