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

package org.apache.iotdb.tool.core.model;

import java.util.List;

public class AnalysedResultModel {

  // 当前编码分析
  private EncodeCompressAnalysedModel currentAnalysed;

  // 所有编码分析列表（直接按序展示）
  private List<EncodeCompressAnalysedModel> analysedList;

  public EncodeCompressAnalysedModel getCurrentAnalysed() {
    return currentAnalysed;
  }

  public void setCurrentAnalysed(EncodeCompressAnalysedModel currentAnalysed) {
    this.currentAnalysed = currentAnalysed;
  }

  public List<EncodeCompressAnalysedModel> getAnalysedList() {
    return analysedList;
  }

  public void setAnalysedList(List<EncodeCompressAnalysedModel> analysedList) {
    this.analysedList = analysedList;
  }
}
