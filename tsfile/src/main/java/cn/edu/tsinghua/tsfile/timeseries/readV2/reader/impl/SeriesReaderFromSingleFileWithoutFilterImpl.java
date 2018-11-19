package cn.edu.tsinghua.tsfile.timeseries.readV2.reader.impl;

import cn.edu.tsinghua.tsfile.common.utils.ITsRandomAccessFileReader;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import cn.edu.tsinghua.tsfile.timeseries.readV2.common.EncodedSeriesChunkDescriptor;
import cn.edu.tsinghua.tsfile.timeseries.readV2.common.SeriesChunk;
import cn.edu.tsinghua.tsfile.timeseries.readV2.controller.SeriesChunkLoader;

import java.io.IOException;
import java.util.List;

/**
 * Created by zhangjinrui on 2017/12/25.
 */
public class SeriesReaderFromSingleFileWithoutFilterImpl extends SeriesReaderFromSingleFile {

    public SeriesReaderFromSingleFileWithoutFilterImpl(SeriesChunkLoader seriesChunkLoader, List<EncodedSeriesChunkDescriptor> encodedSeriesChunkDescriptorList) {
        super(seriesChunkLoader, encodedSeriesChunkDescriptorList);
    }

    public SeriesReaderFromSingleFileWithoutFilterImpl(ITsRandomAccessFileReader randomAccessFileReader, Path path) throws IOException {
        super(randomAccessFileReader, path);
    }

    public SeriesReaderFromSingleFileWithoutFilterImpl(ITsRandomAccessFileReader randomAccessFileReader,
                                      SeriesChunkLoader seriesChunkLoader, List<EncodedSeriesChunkDescriptor> encodedSeriesChunkDescriptorList) {
        super(randomAccessFileReader, seriesChunkLoader, encodedSeriesChunkDescriptorList);
    }

    protected void initSeriesChunkReader(EncodedSeriesChunkDescriptor encodedSeriesChunkDescriptor) throws IOException {
        SeriesChunk memSeriesChunk = seriesChunkLoader.getMemSeriesChunk(encodedSeriesChunkDescriptor);
        this.seriesChunkReader = new SeriesChunkReaderWithoutFilterImpl(memSeriesChunk.getSeriesChunkBodyStream(),
                memSeriesChunk.getEncodedSeriesChunkDescriptor().getDataType(),
                memSeriesChunk.getEncodedSeriesChunkDescriptor().getCompressionTypeName());
        this.seriesChunkReader.setMaxTombstoneTime(encodedSeriesChunkDescriptor.getMaxTombstoneTime());
    }

    @Override
    protected boolean seriesChunkSatisfied(EncodedSeriesChunkDescriptor encodedSeriesChunkDescriptor) {
        return true;
    }
}
