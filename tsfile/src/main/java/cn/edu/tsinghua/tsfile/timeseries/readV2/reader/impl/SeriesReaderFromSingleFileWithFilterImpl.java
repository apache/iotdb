package cn.edu.tsinghua.tsfile.timeseries.readV2.reader.impl;

import cn.edu.tsinghua.tsfile.common.constant.StatisticConstant;
import cn.edu.tsinghua.tsfile.common.utils.ITsRandomAccessFileReader;
import cn.edu.tsinghua.tsfile.timeseries.filter.utils.DigestForFilter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.basic.Filter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.visitor.impl.DigestFilterVisitor;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import cn.edu.tsinghua.tsfile.timeseries.readV2.common.EncodedSeriesChunkDescriptor;
import cn.edu.tsinghua.tsfile.timeseries.readV2.common.SeriesChunk;
import cn.edu.tsinghua.tsfile.timeseries.readV2.controller.SeriesChunkLoader;

import java.io.IOException;
import java.util.List;

/**
 * Created by zhangjinrui on 2017/12/25.
 */
public class SeriesReaderFromSingleFileWithFilterImpl extends SeriesReaderFromSingleFile {

    private Filter<?> filter;
    private DigestFilterVisitor digestFilterVisitor;

    public SeriesReaderFromSingleFileWithFilterImpl(SeriesChunkLoader seriesChunkLoader
            , List<EncodedSeriesChunkDescriptor> encodedSeriesChunkDescriptorList, Filter<?> filter) {
        super(seriesChunkLoader, encodedSeriesChunkDescriptorList);
        this.filter = filter;
        this.digestFilterVisitor = new DigestFilterVisitor();
    }

    public SeriesReaderFromSingleFileWithFilterImpl(ITsRandomAccessFileReader randomAccessFileReader, SeriesChunkLoader seriesChunkLoader,
                                                    List<EncodedSeriesChunkDescriptor> encodedSeriesChunkDescriptorList, Filter<?> filter) {
        super(randomAccessFileReader, seriesChunkLoader, encodedSeriesChunkDescriptorList);
        this.filter = filter;
        this.digestFilterVisitor = new DigestFilterVisitor();
    }

    public SeriesReaderFromSingleFileWithFilterImpl(ITsRandomAccessFileReader randomAccessFileReader
            , Path path, Filter<?> filter) throws IOException {
        super(randomAccessFileReader, path);
        this.filter = filter;
        this.digestFilterVisitor = new DigestFilterVisitor();
    }

    protected void initSeriesChunkReader(EncodedSeriesChunkDescriptor encodedSeriesChunkDescriptor) throws IOException {
        SeriesChunk memSeriesChunk = seriesChunkLoader.getMemSeriesChunk(encodedSeriesChunkDescriptor);
        this.seriesChunkReader = new SeriesChunkReaderWithFilterImpl(memSeriesChunk.getSeriesChunkBodyStream(),
                memSeriesChunk.getEncodedSeriesChunkDescriptor().getDataType(),
                memSeriesChunk.getEncodedSeriesChunkDescriptor().getCompressionTypeName(),
                filter);
        this.seriesChunkReader.setMaxTombstoneTime(encodedSeriesChunkDescriptor.getMaxTombstoneTime());
    }

    @Override
    protected boolean seriesChunkSatisfied(EncodedSeriesChunkDescriptor encodedSeriesChunkDescriptor) {
        DigestForFilter timeDigest = new DigestForFilter(encodedSeriesChunkDescriptor.getMinTimestamp(),
                encodedSeriesChunkDescriptor.getMaxTimestamp());
        //TODO: Using ByteBuffer as min/max is best
        DigestForFilter valueDigest = new DigestForFilter(
                encodedSeriesChunkDescriptor.getValueDigest().getStatistics().get(StatisticConstant.MIN_VALUE),
                encodedSeriesChunkDescriptor.getValueDigest().getStatistics().get(StatisticConstant.MAX_VALUE),
                encodedSeriesChunkDescriptor.getDataType());
        return digestFilterVisitor.satisfy(timeDigest, valueDigest, filter);
    }
}
