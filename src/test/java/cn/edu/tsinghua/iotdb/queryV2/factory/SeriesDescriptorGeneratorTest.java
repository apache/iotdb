package cn.edu.tsinghua.iotdb.queryV2.factory;

import cn.edu.tsinghua.iotdb.engine.querycontext.OverflowInsertFile;
import cn.edu.tsinghua.tsfile.file.metadata.TInTimeSeriesChunkMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.TimeSeriesChunkMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.TimeSeriesChunkProperties;
import cn.edu.tsinghua.tsfile.file.metadata.VInTimeSeriesChunkMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.enums.CompressionTypeName;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.readV2.common.EncodedSeriesChunkDescriptor;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by zhangjinrui on 2018/1/18.
 */
public class SeriesDescriptorGeneratorTest {

    @Test
    public void test() {
        String filePath = "testPath";
        long offset = 1000;
        long startTime = 1L;
        long endTime = 2L;
        int numRows = 1000;

        List<OverflowInsertFile> overflowInsertFileList = new ArrayList<>();
        OverflowInsertFile overflowInsertFile = new OverflowInsertFile();
        overflowInsertFile.setPath(filePath);
        List<TimeSeriesChunkMetaData> timeSeriesChunkMetaDatas = new ArrayList<>();
        TimeSeriesChunkMetaData timeSeriesChunkMetaData = new TimeSeriesChunkMetaData();
        timeSeriesChunkMetaData.setProperties(new TimeSeriesChunkProperties("m1", null, offset, CompressionTypeName.GZIP));
        timeSeriesChunkMetaData.setVInTimeSeriesChunkMetaData(new VInTimeSeriesChunkMetaData(TSDataType.BOOLEAN));
        timeSeriesChunkMetaData.setTInTimeSeriesChunkMetaData(new TInTimeSeriesChunkMetaData(TSDataType.BOOLEAN, startTime, endTime));
        timeSeriesChunkMetaData.setNumRows(numRows);
        timeSeriesChunkMetaDatas.add(timeSeriesChunkMetaData);

        overflowInsertFile.setTimeSeriesChunkMetaDatas(timeSeriesChunkMetaDatas);
        overflowInsertFileList.add(overflowInsertFile);

        List<EncodedSeriesChunkDescriptor> seriesChunkDescriptors = SeriesDescriptorGenerator.genSeriesChunkDescriptorList(overflowInsertFileList);
        Assert.assertEquals(1, seriesChunkDescriptors.size());
        EncodedSeriesChunkDescriptor seriesChunkDescriptor = seriesChunkDescriptors.get(0);
        Assert.assertEquals(filePath, seriesChunkDescriptor.getFilePath());
        Assert.assertEquals(startTime, seriesChunkDescriptor.getMinTimestamp());
        Assert.assertEquals(endTime, seriesChunkDescriptor.getMaxTimestamp());
        Assert.assertEquals(numRows, seriesChunkDescriptor.getCountOfPoints());
    }
}
