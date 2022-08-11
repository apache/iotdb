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
