package org.apache.iotdb.tool.core.test;

import org.apache.iotdb.tool.core.model.PageInfo;
import org.apache.iotdb.tool.core.service.TsFileAnalyserV13;
import org.apache.iotdb.tool.core.util.OffLineTsFileUtil;
import org.apache.iotdb.tsfile.read.common.BatchData;

import java.io.IOException;
import java.util.List;

public class TsFilePageBatchDataTest {
  public static void main(String[] args) throws IOException {
    TsFileAnalyserV13 tsFileAnalyserV13 = new TsFileAnalyserV13("./1650952199003-7-0-0.tsfile");
    System.out.println(OffLineTsFileUtil.fetchTsFileVersionNumber("./1650952199003-7-0-0.tsfile"));

    List<PageInfo> pageInfoList =
        tsFileAnalyserV13.fetchPageInfoListByChunkMetadata(
            tsFileAnalyserV13
                .getChunkGroupMetadataModelList()
                .get(0)
                .getChunkMetadataList()
                .get(0));
    for (PageInfo pageInfo : pageInfoList) {
      BatchData batchData = tsFileAnalyserV13.fetchBatchDataByPageInfo(pageInfo);
      while (batchData.hasCurrent()) {
        System.out.println(
            "time :" + batchData.currentTime() + " value:" + batchData.currentValue());
        batchData.next();
      }
    }
  }
}
