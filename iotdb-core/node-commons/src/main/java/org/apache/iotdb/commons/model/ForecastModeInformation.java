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

import org.apache.iotdb.common.rpc.thrift.ModelTask;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import javax.annotation.Nullable;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class ForecastModeInformation extends ModelInformation {

  private final List<TSDataType> inputTypeList;

  private final List<Integer> predictIndexList;

  private final int inputLength;
  private final int predictLength;

  public ForecastModeInformation(
      String modelId,
      String modelType,
      boolean isAuto,
      List<String> queryExpressions,
      @Nullable String queryFilter,
      List<TSDataType> inputTypeList,
      List<Integer> predictIndexList,
      int inputLength,
      int predictLength) {
    super(ModelTask.FORECAST, modelId, modelType, isAuto, queryExpressions, queryFilter);
    this.inputTypeList = inputTypeList;
    this.predictIndexList = predictIndexList;
    this.inputLength = inputLength;
    this.predictLength = predictLength;
  }

  public ForecastModeInformation(ByteBuffer buffer) {
    super(ModelTask.FORECAST, buffer);
    int listSize = ReadWriteIOUtils.readInt(buffer);
    this.inputTypeList = new ArrayList<>(listSize);
    for (int i = 0; i < listSize; i++) {
      inputTypeList.add(TSDataType.deserializeFrom(buffer));
    }
    listSize = ReadWriteIOUtils.readInt(buffer);
    this.predictIndexList = new ArrayList<>(listSize);
    for (int i = 0; i < listSize; i++) {
      predictIndexList.add(ReadWriteIOUtils.readInt(buffer));
    }
    this.inputLength = ReadWriteIOUtils.readInt(buffer);
    this.predictLength = ReadWriteIOUtils.readInt(buffer);
  }

  public ForecastModeInformation(InputStream stream) throws IOException {
    super(ModelTask.FORECAST, stream);
    int listSize = ReadWriteIOUtils.readInt(stream);
    this.inputTypeList = new ArrayList<>(listSize);
    for (int i = 0; i < listSize; i++) {
      inputTypeList.add(TSDataType.deserializeFrom(stream));
    }
    listSize = ReadWriteIOUtils.readInt(stream);
    this.predictIndexList = new ArrayList<>(listSize);
    for (int i = 0; i < listSize; i++) {
      predictIndexList.add(ReadWriteIOUtils.readInt(stream));
    }
    this.inputLength = ReadWriteIOUtils.readInt(stream);
    this.predictLength = ReadWriteIOUtils.readInt(stream);
  }

  public List<TSDataType> getInputTypeList() {
    return inputTypeList;
  }

  public List<Integer> getPredictIndexList() {
    return predictIndexList;
  }

  public int getInputLength() {
    return inputLength;
  }

  public int getPredictLength() {
    return predictLength;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    super.serialize(stream);
    ReadWriteIOUtils.write(inputTypeList.size(), stream);
    for (TSDataType inputType : inputTypeList) {
      inputType.serializeTo(stream);
    }
    ReadWriteIOUtils.write(predictIndexList.size(), stream);
    for (Integer index : predictIndexList) {
      ReadWriteIOUtils.write(index, stream);
    }
    ReadWriteIOUtils.write(inputLength, stream);
    ReadWriteIOUtils.write(predictLength, stream);
  }

  @Override
  public void serialize(FileOutputStream stream) throws IOException {
    super.serialize(stream);
    ReadWriteIOUtils.write(inputTypeList.size(), stream);
    for (TSDataType inputType : inputTypeList) {
      inputType.serializeTo(stream);
    }
    ReadWriteIOUtils.write(predictIndexList.size(), stream);
    for (Integer index : predictIndexList) {
      ReadWriteIOUtils.write(index, stream);
    }
    ReadWriteIOUtils.write(inputLength, stream);
    ReadWriteIOUtils.write(predictLength, stream);
  }
}
