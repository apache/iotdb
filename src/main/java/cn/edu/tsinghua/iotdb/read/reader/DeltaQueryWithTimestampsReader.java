package cn.edu.tsinghua.iotdb.read.reader;

import cn.edu.tsinghua.tsfile.common.utils.ITsRandomAccessFileReader;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import cn.edu.tsinghua.tsfile.timeseries.readV2.common.EncodedSeriesChunkDescriptor;
import cn.edu.tsinghua.tsfile.timeseries.readV2.controller.SeriesChunkLoader;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.impl.SeriesReaderFromSingleFileByTimestampImpl;

import java.io.IOException;
import java.util.List;

public class DeltaQueryWithTimestampsReader extends SeriesReaderFromSingleFileByTimestampImpl {

    public DeltaQueryWithTimestampsReader(SeriesChunkLoader seriesChunkLoader, List<EncodedSeriesChunkDescriptor> encodedSeriesChunkDescriptorList) {
        super(seriesChunkLoader, encodedSeriesChunkDescriptorList);
    }

    public DeltaQueryWithTimestampsReader(ITsRandomAccessFileReader randomAccessFileReader, Path path) throws IOException {
        super(randomAccessFileReader, path);
    }

    public DeltaQueryWithTimestampsReader(ITsRandomAccessFileReader randomAccessFileReader, SeriesChunkLoader seriesChunkLoader, List<EncodedSeriesChunkDescriptor> encodedSeriesChunkDescriptorList) {
        super(randomAccessFileReader, seriesChunkLoader, encodedSeriesChunkDescriptorList);
    }

}
