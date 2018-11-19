package cn.edu.tsinghua.iotdb.queryV2.engine.control;

import cn.edu.tsinghua.tsfile.timeseries.readV2.common.EncodedSeriesChunkDescriptor;
import cn.edu.tsinghua.tsfile.timeseries.readV2.common.SeriesChunk;
import cn.edu.tsinghua.tsfile.timeseries.readV2.controller.SeriesChunkLoader;

import java.io.IOException;

/**
 * Created by zhangjinrui on 2018/1/24.
 */
public class SeriesChunkLoaderIoTDBImpl implements SeriesChunkLoader{

    @Override
    public SeriesChunk getMemSeriesChunk(EncodedSeriesChunkDescriptor encodedSeriesChunkDescriptor) throws IOException {
        return null;
    }
}
