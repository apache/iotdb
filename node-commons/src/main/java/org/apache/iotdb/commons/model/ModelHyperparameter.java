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

import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Map;

public class ModelHyperparameter {

  private final Map<String, String> keyValueMap;

  public ModelHyperparameter(Map<String, String> keyValueMap) {
    this.keyValueMap = keyValueMap;
  }

  public void update(Map<String, String> modelInfo) {
    this.keyValueMap.putAll(modelInfo);
  }

  @Override
  public String toString() {
    StringBuilder stringBuilder = new StringBuilder();
    for (Map.Entry<String, String> keyValuePair : keyValueMap.entrySet()) {
      stringBuilder
          .append(keyValuePair.getKey())
          .append('=')
          .append(keyValuePair.getValue())
          .append('\n');
    }
    return stringBuilder.toString();
  }

  public void serialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(keyValueMap, stream);
  }

  public void serialize(FileOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(keyValueMap, stream);
  }

  public static ModelHyperparameter deserialize(ByteBuffer buffer) {
    return new ModelHyperparameter(ReadWriteIOUtils.readMap(buffer));
  }

  public static ModelHyperparameter deserialize(InputStream stream) throws IOException {
    return new ModelHyperparameter(ReadWriteIOUtils.readMap(stream));
  }
}
