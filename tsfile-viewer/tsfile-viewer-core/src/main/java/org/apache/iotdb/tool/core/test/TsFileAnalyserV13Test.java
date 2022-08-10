package org.apache.iotdb.tool.core.test;

import org.apache.iotdb.tool.core.model.ChunkModel;
import org.apache.iotdb.tool.core.service.TsFileAnalyserV13;
import org.apache.iotdb.tool.core.util.OffLineTsFileUtil;
import org.apache.iotdb.tsfile.read.common.BatchData;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.concurrent.TimeUnit;

public class TsFileAnalyserV13Test {

    public static void main(String[] strings) throws IOException, InterruptedException {
    TsFileAnalyserV13 tsFileAnalyserV13 = new TsFileAnalyserV13("1652336687038-87274-2-5.tsfile");

    System.out.println(OffLineTsFileUtil.fetchTsFileVersionNumber("1652336687038-87274-2-5.tsfile"));

    while (tsFileAnalyserV13.getRateOfProcess() < 1.0) {
        BigDecimal bd = new BigDecimal(tsFileAnalyserV13.getRateOfProcess());
        System.out.println("load process :" + bd.setScale(5, BigDecimal.ROUND_DOWN).doubleValue());
        TimeUnit.MICROSECONDS.sleep(10);
    }
    ChunkModel chunk =
            tsFileAnalyserV13.fetchChunkByChunkMetadata(
                    tsFileAnalyserV13.getChunkGroupMetadataModelList().get(0).getChunkMetadataList().get(0));
    BatchData data = chunk.getBatchDataList().get(0);
    while (data.hasCurrent()) {
        System.out.println("time :" + data.currentTime() + " value:" + data.currentValue());
        data.next();
    }
    tsFileAnalyserV13.queryResult(1652337640062l, 0, "root.ln.test.tag", "value", "", 0, 0);
    }
}
