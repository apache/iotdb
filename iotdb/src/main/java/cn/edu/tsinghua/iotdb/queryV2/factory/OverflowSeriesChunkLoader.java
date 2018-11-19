package cn.edu.tsinghua.iotdb.queryV2.factory;

import cn.edu.tsinghua.iotdb.queryV2.engine.control.OverflowFileStreamManager;
import cn.edu.tsinghua.iotdb.queryV2.engine.reader.component.BufferedSeriesChunk;
import cn.edu.tsinghua.iotdb.queryV2.engine.reader.component.SegmentInputStream;
import cn.edu.tsinghua.iotdb.queryV2.engine.reader.component.SegmentInputStreamWithMMap;
import cn.edu.tsinghua.tsfile.timeseries.readV2.common.EncodedSeriesChunkDescriptor;
import cn.edu.tsinghua.tsfile.timeseries.readV2.common.SeriesChunk;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;

/**
 * This class is used to load one SeriesChunk according to the SeriesChunkDescriptor
 * Created by zhangjinrui on 2018/1/18.
 */
public class OverflowSeriesChunkLoader {
    private static final Logger logger = LoggerFactory.getLogger(OverflowSeriesChunkLoader.class);
    private OverflowFileStreamManager overflowFileStreamManager;

    public OverflowSeriesChunkLoader() {
        overflowFileStreamManager = OverflowFileStreamManager.getInstance();
    }

    public SeriesChunk getMemSeriesChunk(Long jobId, EncodedSeriesChunkDescriptor scDescriptor) throws IOException {
        if (overflowFileStreamManager.contains(scDescriptor.getFilePath()) || (new File(scDescriptor.getFilePath()).length() +
                overflowFileStreamManager.getMappedByteBufferUsage().get()  < Integer.MAX_VALUE)) {
            MappedByteBuffer buffer = overflowFileStreamManager.get(scDescriptor.getFilePath());
            return new BufferedSeriesChunk(
                    new SegmentInputStreamWithMMap(buffer, scDescriptor.getOffsetInFile(), scDescriptor.getLengthOfBytes()),
                    scDescriptor);
        } else {
            RandomAccessFile randomAccessFile = overflowFileStreamManager.get(jobId, scDescriptor.getFilePath());
            return new BufferedSeriesChunk(
                    new SegmentInputStream(randomAccessFile, scDescriptor.getOffsetInFile(), scDescriptor.getLengthOfBytes()),
                    scDescriptor);
        }
    }
}
