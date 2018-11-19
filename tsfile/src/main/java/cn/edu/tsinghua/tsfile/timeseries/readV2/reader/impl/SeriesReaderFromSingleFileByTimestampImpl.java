package cn.edu.tsinghua.tsfile.timeseries.readV2.reader.impl;

import cn.edu.tsinghua.tsfile.common.utils.ITsRandomAccessFileReader;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import cn.edu.tsinghua.tsfile.timeseries.readV2.common.EncodedSeriesChunkDescriptor;
import cn.edu.tsinghua.tsfile.timeseries.readV2.common.SeriesChunk;
import cn.edu.tsinghua.tsfile.timeseries.readV2.controller.SeriesChunkLoader;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TimeValuePair;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TsPrimitiveType;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.SeriesReaderByTimeStamp;

import java.io.IOException;
import java.util.List;

/**
 * Created by zhangjinrui on 2017/12/26.
 */
public class SeriesReaderFromSingleFileByTimestampImpl extends SeriesReaderFromSingleFile implements SeriesReaderByTimeStamp {

    private long currentTimestamp;
    private boolean hasCacheLastTimeValuePair;
    private TimeValuePair cachedTimeValuePair;
    private int nextSeriesChunkIndex;

    public SeriesReaderFromSingleFileByTimestampImpl(SeriesChunkLoader seriesChunkLoader, List<EncodedSeriesChunkDescriptor> encodedSeriesChunkDescriptorList) {
        super(seriesChunkLoader, encodedSeriesChunkDescriptorList);
        nextSeriesChunkIndex = 0;
        currentTimestamp = Long.MIN_VALUE;
    }

    public SeriesReaderFromSingleFileByTimestampImpl(ITsRandomAccessFileReader randomAccessFileReader, Path path) throws IOException {
        super(randomAccessFileReader, path);
        currentTimestamp = Long.MIN_VALUE;
    }

    public SeriesReaderFromSingleFileByTimestampImpl(ITsRandomAccessFileReader randomAccessFileReader,
                                      SeriesChunkLoader seriesChunkLoader, List<EncodedSeriesChunkDescriptor> encodedSeriesChunkDescriptorList) {
        super(randomAccessFileReader, seriesChunkLoader, encodedSeriesChunkDescriptorList);
        currentTimestamp = Long.MIN_VALUE;
    }

    @Override
    public boolean hasNext() throws IOException {
        if (hasCacheLastTimeValuePair && cachedTimeValuePair.getTimestamp() >= currentTimestamp) {
            return true;
        }
        if (seriesChunkReaderInitialized) {
            ((SeriesChunkReaderByTimestampImpl) seriesChunkReader).setCurrentTimestamp(currentTimestamp);
            if(seriesChunkReader.hasNext()){
                return true;
            }
        }
        while (nextSeriesChunkIndex < encodedSeriesChunkDescriptorList.size()) {
            if (!seriesChunkReaderInitialized) {
                EncodedSeriesChunkDescriptor encodedSeriesChunkDescriptor = encodedSeriesChunkDescriptorList.get(nextSeriesChunkIndex);
                //maxTime >= currentTime
                if (seriesChunkSatisfied(encodedSeriesChunkDescriptor)) {
                    initSeriesChunkReader(encodedSeriesChunkDescriptor);
                    ((SeriesChunkReaderByTimestampImpl) seriesChunkReader).setCurrentTimestamp(currentTimestamp);
                    seriesChunkReaderInitialized = true;
                    nextSeriesChunkIndex++;
                } else {
                    //maxTime < currentTime, skip this seriesChunk
                    continue;
                }
            }
            if (seriesChunkReader.hasNext()) {
                return true;
            } else {
                seriesChunkReaderInitialized = false;
            }
        }
        return false;
    }

    @Override
    public TimeValuePair next() throws IOException {
        if (hasCacheLastTimeValuePair) {
            hasCacheLastTimeValuePair = false;
            return cachedTimeValuePair;
        }
        return seriesChunkReader.next();
    }

    /**
     * @param timestamp
     * @return If there is no TimeValuePair whose timestamp equals to given timestamp, then return null.
     * @throws IOException
     */
    @Override
    public TsPrimitiveType getValueInTimestamp(long timestamp) throws IOException {
        this.currentTimestamp = timestamp;
        if (hasCacheLastTimeValuePair) {
            if (cachedTimeValuePair.getTimestamp() == timestamp) {
                hasCacheLastTimeValuePair = false;
                return cachedTimeValuePair.getValue();
            } else if (cachedTimeValuePair.getTimestamp() > timestamp) {
                return null;
            }
        }
        if(hasNext()){
            cachedTimeValuePair = next();
            if (cachedTimeValuePair.getTimestamp() == timestamp) {
                return cachedTimeValuePair.getValue();
            } else if (cachedTimeValuePair.getTimestamp() > timestamp) {
                hasCacheLastTimeValuePair = true;
                return null;
            }
        }
        return null;
    }

    @Override
    protected void initSeriesChunkReader(EncodedSeriesChunkDescriptor encodedSeriesChunkDescriptor) throws IOException {
        SeriesChunk memSeriesChunk = seriesChunkLoader.getMemSeriesChunk(encodedSeriesChunkDescriptor);
        this.seriesChunkReader = new SeriesChunkReaderByTimestampImpl(memSeriesChunk.getSeriesChunkBodyStream()
                , encodedSeriesChunkDescriptor.getDataType(),
                encodedSeriesChunkDescriptor.getCompressionTypeName());
        this.seriesChunkReader.setMaxTombstoneTime(encodedSeriesChunkDescriptor.getMaxTombstoneTime());
    }

    @Override
    protected boolean seriesChunkSatisfied(EncodedSeriesChunkDescriptor encodedSeriesChunkDescriptor) {
        long maxTimestamp = encodedSeriesChunkDescriptor.getMaxTimestamp();
        return  maxTimestamp >= currentTimestamp;
    }
}
