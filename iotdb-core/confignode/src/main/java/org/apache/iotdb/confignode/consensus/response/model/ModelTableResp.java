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

package org.apache.iotdb.confignode.consensus.response.model;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.model.ModelInformation;
import org.apache.iotdb.consensus.common.DataSet;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

// TODO: Will be removed in the future
public class ModelTableResp implements DataSet {

  private final TSStatus status;
  private final List<ByteBuffer> serializedAllModelInformation;
  private Map<String, String> modelTypeMap;
  private Map<String, String> algorithmMap;

  public ModelTableResp(TSStatus status) {
    this.status = status;
    this.serializedAllModelInformation = new ArrayList<>();
  }

  public void addModelInformation(List<ModelInformation> modelInformationList) throws IOException {
    for (ModelInformation modelInformation : modelInformationList) {
      this.serializedAllModelInformation.add(modelInformation.serializeShowModelResult());
    }
  }

  public void addModelInformation(ModelInformation modelInformation) throws IOException {
    this.serializedAllModelInformation.add(modelInformation.serializeShowModelResult());
  }

  public void setModelTypeMap(Map<String, String> modelTypeMap) {
    this.modelTypeMap = modelTypeMap;
  }

  public void setAlgorithmMap(Map<String, String> algorithmMap) {
    this.algorithmMap = algorithmMap;
  }
}
