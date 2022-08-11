package org.apache.iotdb.tool.core.test;

import org.apache.iotdb.tool.core.model.*;
import org.apache.iotdb.tool.core.service.TsFileAnalyserV13;
import org.apache.iotdb.tool.core.util.OffLineTsFileUtil;

import java.io.IOException;
import java.util.List;

public class TsFileAnalyserV13Test {

  public static void main(String[] strings) throws IOException, InterruptedException {
    TsFileAnalyserV13 tsFileAnalyserV13 = new TsFileAnalyserV13("1652336687038-87274-2-5.tsfile");

    System.out.println(
        OffLineTsFileUtil.fetchTsFileVersionNumber("1652336687038-87274-2-5.tsfile"));

    TimeSeriesMetadataNode node = tsFileAnalyserV13.getTimeSeriesMetadataNode();
    List<ChunkGroupMetadataModel> modelList = tsFileAnalyserV13.getChunkGroupMetadataModelList();
    List<IPageInfo> pageInfosList =
        tsFileAnalyserV13.fetchPageInfoListByIChunkMetadata(
            modelList.get(0).getChunkMetadataList().get(0));

    AnalysedResultModel resultModel =
        tsFileAnalyserV13.fetchAnalysedResultWithDeviceAndMeasurement(
            modelList.get(0).getDevice(),
            modelList.get(0).getChunkMetadataList().get(0).getMeasurementUid());
    System.out.println(resultModel.getCurrentAnalysed().toString());
    resultModel.getAnalysedList().forEach(System.out::println);
  }
}
