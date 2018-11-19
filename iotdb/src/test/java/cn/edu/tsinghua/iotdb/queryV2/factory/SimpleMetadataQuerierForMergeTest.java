package cn.edu.tsinghua.iotdb.queryV2.factory;

import cn.edu.tsinghua.iotdb.queryV2.TsFileGenerator;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import cn.edu.tsinghua.tsfile.timeseries.readV2.common.EncodedSeriesChunkDescriptor;
import cn.edu.tsinghua.tsfile.timeseries.write.exception.WriteProcessException;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * Created by zhangjinrui on 2018/2/1.
 */
public class SimpleMetadataQuerierForMergeTest {

    @Test
    public void test() throws InterruptedException, WriteProcessException, IOException {
        String path = "SimpleMetadataQuerierForMergeTest.ts";
        TsFileGenerator.write(path, 10000, 1 * 1024 * 1024, 50000);

        SimpleMetadataQuerierForMerge metadataQuerierForMerge = new SimpleMetadataQuerierForMerge(path);
        List<EncodedSeriesChunkDescriptor> descriptorList = metadataQuerierForMerge.getSeriesChunkDescriptorList(new Path("d1.s1"));
        for(EncodedSeriesChunkDescriptor encodedSeriesChunkDescriptor : descriptorList) {
            Assert.assertEquals(TSDataType.INT32, encodedSeriesChunkDescriptor.getDataType());
        }
        Assert.assertNotEquals(0, descriptorList.size());
        descriptorList = metadataQuerierForMerge.getSeriesChunkDescriptorList(new Path("d2.s4"));
        for(EncodedSeriesChunkDescriptor encodedSeriesChunkDescriptor : descriptorList) {
            Assert.assertEquals(TSDataType.TEXT, encodedSeriesChunkDescriptor.getDataType());
        }
        Assert.assertNotEquals(0, descriptorList.size());
        TsFileGenerator.after();
    }
}
