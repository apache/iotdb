package cn.edu.tsinghua.tsfile.timeseries.readV2.reader.impl;

import cn.edu.tsinghua.tsfile.common.utils.ITsRandomAccessFileReader;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import cn.edu.tsinghua.tsfile.timeseries.readV2.common.EncodedSeriesChunkDescriptor;
import cn.edu.tsinghua.tsfile.timeseries.readV2.controller.MetadataQuerierByFileImpl;
import cn.edu.tsinghua.tsfile.timeseries.readV2.controller.SeriesChunkLoader;
import cn.edu.tsinghua.tsfile.timeseries.readV2.controller.SeriesChunkLoaderImpl;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TimeValuePair;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.SeriesReader;

import java.io.IOException;
import java.util.List;

/**
 * Created by zhangjinrui on 2017/12/25.
 */
public abstract class SeriesReaderFromSingleFile implements SeriesReader {

    protected SeriesChunkLoader seriesChunkLoader;
    protected List<EncodedSeriesChunkDescriptor> encodedSeriesChunkDescriptorList;

    protected SeriesChunkReader seriesChunkReader;
    protected boolean seriesChunkReaderInitialized;
    protected int currentReadSeriesChunkIndex;

    protected ITsRandomAccessFileReader randomAccessFileReader;

    public SeriesReaderFromSingleFile(ITsRandomAccessFileReader randomAccessFileReader, Path path) throws IOException {
        this.randomAccessFileReader = randomAccessFileReader;
        this.seriesChunkLoader = new SeriesChunkLoaderImpl(randomAccessFileReader);
        this.encodedSeriesChunkDescriptorList = new MetadataQuerierByFileImpl(randomAccessFileReader).getSeriesChunkDescriptorList(path);
        this.currentReadSeriesChunkIndex = -1;
        this.seriesChunkReaderInitialized = false;
    }

    public SeriesReaderFromSingleFile(ITsRandomAccessFileReader randomAccessFileReader,
                                      SeriesChunkLoader seriesChunkLoader, List<EncodedSeriesChunkDescriptor> encodedSeriesChunkDescriptorList) {
        this(seriesChunkLoader, encodedSeriesChunkDescriptorList);
        this.randomAccessFileReader = randomAccessFileReader;
    }

    /**
     * Using this constructor cannot close corresponding FileStream
     * @param seriesChunkLoader
     * @param encodedSeriesChunkDescriptorList
     */
    public SeriesReaderFromSingleFile(SeriesChunkLoader seriesChunkLoader, List<EncodedSeriesChunkDescriptor> encodedSeriesChunkDescriptorList) {
        this.seriesChunkLoader = seriesChunkLoader;
        this.encodedSeriesChunkDescriptorList = encodedSeriesChunkDescriptorList;
        this.currentReadSeriesChunkIndex = -1;
        this.seriesChunkReaderInitialized = false;
    }

    @Override
    public boolean hasNext() throws IOException {
        if (seriesChunkReaderInitialized && seriesChunkReader.hasNext()) {
            return true;
        }
        while ((currentReadSeriesChunkIndex + 1) < encodedSeriesChunkDescriptorList.size()) {
            if (!seriesChunkReaderInitialized) {
                EncodedSeriesChunkDescriptor encodedSeriesChunkDescriptor = encodedSeriesChunkDescriptorList.get(++currentReadSeriesChunkIndex);
                if (seriesChunkSatisfied(encodedSeriesChunkDescriptor)) {
                    initSeriesChunkReader(encodedSeriesChunkDescriptor);
                    seriesChunkReaderInitialized = true;
                } else {
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
        return seriesChunkReader.next();
    }

    @Override
    public void skipCurrentTimeValuePair() throws IOException {
        next();
    }

    protected abstract void initSeriesChunkReader(EncodedSeriesChunkDescriptor encodedSeriesChunkDescriptor) throws IOException;

    protected abstract boolean seriesChunkSatisfied(EncodedSeriesChunkDescriptor encodedSeriesChunkDescriptor);

    public void close() throws IOException {
        if (randomAccessFileReader != null) {
            randomAccessFileReader.close();
        }
    }
}
