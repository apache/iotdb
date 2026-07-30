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

package org.apache.iotdb.db.queryengine.plan.analyze;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.model.ModelInformation;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.model.ModelInferenceDescriptor;
import org.apache.iotdb.rpc.TSStatusCode;

// TODO: This class should contact with AINode directly and cache model info in DataNode
public class ModelFetcher implements IModelFetcher {

  private static final class ModelFetcherHolder {

    private static final ModelFetcher INSTANCE = new ModelFetcher();

    private ModelFetcherHolder() {}
  }

  public static ModelFetcher getInstance() {
    return ModelFetcherHolder.INSTANCE;
  }

  private ModelFetcher() {}

  @Override
  public TSStatus fetchModel(String modelId, Analysis analysis) {
    analysis.setModelInferenceDescriptor(
        new ModelInferenceDescriptor(new ModelInformation(modelId)));
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }
}
