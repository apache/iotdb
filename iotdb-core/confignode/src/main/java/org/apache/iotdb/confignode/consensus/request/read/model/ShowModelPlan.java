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

package org.apache.iotdb.confignode.consensus.request.read.model;

import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.confignode.consensus.request.read.ConfigPhysicalReadPlan;
import org.apache.iotdb.confignode.rpc.thrift.TShowModelReq;

import java.util.Objects;

public class ShowModelPlan extends ConfigPhysicalReadPlan {

  private String modelName;

  public ShowModelPlan(final TShowModelReq showModelReq) {
    super(ConfigPhysicalPlanType.ShowModel);
    if (showModelReq.isSetModelId()) {
      this.modelName = showModelReq.getModelId();
    }
  }

  public boolean isSetModelName() {
    return modelName != null;
  }

  public String getModelName() {
    return modelName;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    final ShowModelPlan that = (ShowModelPlan) o;
    return Objects.equals(modelName, that.modelName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), modelName);
  }
}
