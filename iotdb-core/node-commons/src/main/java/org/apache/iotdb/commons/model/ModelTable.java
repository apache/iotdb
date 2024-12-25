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

package org.apache.iotdb.commons.model;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ModelTable {

  private final Map<String, ModelInformation> modelInfoMap;

  public ModelTable() {
    this.modelInfoMap = new ConcurrentHashMap<>();
  }

  public boolean containsModel(String modelId) {
    return modelInfoMap.containsKey(modelId);
  }

  public void addModel(ModelInformation modelInformation) {
    modelInfoMap.put(modelInformation.getModelName(), modelInformation);
  }

  public void removeModel(String modelId) {
    modelInfoMap.remove(modelId);
  }

  public List<ModelInformation> getAllModelInformation() {
    return new ArrayList<>(modelInfoMap.values());
  }

  public ModelInformation getModelInformationById(String modelId) {
    if (modelInfoMap.containsKey(modelId)) {
      return modelInfoMap.get(modelId);
    }
    return null;
  }

  public void clearFailedModel() {
    for (ModelInformation modelInformation : modelInfoMap.values()) {
      if (modelInformation.getStatus() == ModelStatus.UNAVAILABLE) {
        modelInfoMap.remove(modelInformation.getModelName());
      }
    }
  }

  public void updateModel(String modelName, ModelInformation modelInfo) {
    modelInfoMap.replace(modelName, modelInfo);
  }

  public void clear() {
    modelInfoMap.clear();
  }

  public void serialize(FileOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(modelInfoMap.size(), stream);
    for (ModelInformation entry : modelInfoMap.values()) {
      entry.serialize(stream);
    }
  }

  public static ModelTable deserialize(InputStream stream) throws IOException {
    ModelTable modelTable = new ModelTable();
    int size = ReadWriteIOUtils.readInt(stream);
    for (int i = 0; i < size; i++) {
      modelTable.addModel(ModelInformation.deserialize(stream));
    }
    return modelTable;
  }
}
