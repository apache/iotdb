package cn.edu.tsinghua.iotdb.read.reader;

import cn.edu.tsinghua.iotdb.engine.filenode.IntervalFileNode;
import cn.edu.tsinghua.iotdb.engine.querycontext.GlobalSortedSeriesDataSource;
import cn.edu.tsinghua.iotdb.engine.querycontext.UnsealedTsFile;
import cn.edu.tsinghua.iotdb.queryV2.engine.reader.series.RawSeriesChunkReaderByTimestamp;
import cn.edu.tsinghua.tsfile.common.utils.ITsRandomAccessFileReader;
import cn.edu.tsinghua.tsfile.timeseries.read.TsRandomAccessLocalFileReader;
import cn.edu.tsinghua.tsfile.timeseries.readV2.common.EncodedSeriesChunkDescriptor;
import cn.edu.tsinghua.tsfile.timeseries.readV2.controller.SeriesChunkLoader;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TimeValuePair;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TsPrimitiveType;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.SeriesReaderByTimeStamp;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.impl.SeriesReaderFromSingleFileByTimestampImpl;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

public class TsFilesReaderWithTimeStamp extends TsFilesReader implements SeriesReaderByTimeStamp{

    private long currentTimestamp;
    private boolean hasSeriesReaderByTimestampInitialized;
    private int nextSeriesReaderByTimestampIndex;
    private SeriesReaderByTimeStamp currentSeriesByTimestampReader;

    public TsFilesReaderWithTimeStamp(GlobalSortedSeriesDataSource sortedSeriesDataSource)
            throws IOException {
        super(sortedSeriesDataSource);

        //data in sealedTsFiles and unSealedTsFile
        if(sortedSeriesDataSource.getSealedTsFiles() != null){
            seriesReaders.add(new TsFilesReaderWithTimeStamp.SealedTsFileWithTimeStampReader(sortedSeriesDataSource.getSealedTsFiles()));
        }
        if(sortedSeriesDataSource.getUnsealedTsFile() != null){
            seriesReaders.add(new TsFilesReaderWithTimeStamp.UnSealedTsFileWithTimeStampReader(sortedSeriesDataSource.getUnsealedTsFile()));
        }
        //data in memTable
        if(sortedSeriesDataSource.hasRawSeriesChunk()) {
            seriesReaders.add(new RawSeriesChunkReaderByTimestamp(sortedSeriesDataSource.getRawSeriesChunk()));
        }

        hasSeriesReaderByTimestampInitialized = false;
        nextSeriesReaderByTimestampIndex = 0;
    }

    /**
     * @param timestamp
     * @return If there is no TimeValuePair whose timestamp equals to given timestamp, then return null.
     * @throws IOException
     */
    @Override
    public TsPrimitiveType getValueInTimestamp(long timestamp) throws IOException {
        setCurrentTimestamp(timestamp);
        TsPrimitiveType value = null;
        if(hasSeriesReaderByTimestampInitialized){
            value = currentSeriesByTimestampReader.getValueInTimestamp(timestamp);
            if(value != null){
                return value;
            }
            else {
                hasSeriesReaderByTimestampInitialized = false;
            }
        }
        while (nextSeriesReaderByTimestampIndex < seriesReaders.size()){
            if(!hasSeriesReaderByTimestampInitialized){
                currentSeriesByTimestampReader = (SeriesReaderByTimeStamp) seriesReaders.get(nextSeriesReaderByTimestampIndex++);
                hasSeriesReaderByTimestampInitialized = true;
            }
            value = currentSeriesByTimestampReader.getValueInTimestamp(timestamp);
            if(value != null){
                return value;
            }
            else {
                hasSeriesReaderByTimestampInitialized = false;
            }
        }
        return value;
    }


    public void setCurrentTimestamp(long currentTimestamp) {
        this.currentTimestamp = currentTimestamp;
    }

    private class SealedTsFileWithTimeStampReader extends TsFilesReader.SealedTsFileReader implements SeriesReaderByTimeStamp {

        private boolean hasCacheLastTimeValuePair;
        private TimeValuePair cachedTimeValuePair;

        private SealedTsFileWithTimeStampReader(List<IntervalFileNode> sealedTsFiles){
            super(sealedTsFiles);
            hasCacheLastTimeValuePair = false;
        }

        @Override
        public boolean hasNext() throws IOException {
            //hasCached
            if(hasCacheLastTimeValuePair && cachedTimeValuePair.getTimestamp() == currentTimestamp){
                return true;
            }
            //singleTsFileReader has initialized
            if (singleTsFileReaderInitialized) {
                TsPrimitiveType value = ((SeriesReaderFromSingleFileByTimestampImpl)singleTsFileReader).getValueInTimestamp(currentTimestamp);
                if(value != null){
                    hasCacheLastTimeValuePair = true;
                    cachedTimeValuePair = new TimeValuePair(currentTimestamp,value);
                    return true;
                }
                else {
                    singleTsFileReaderInitialized = false;
                }
            }

            while ((usedIntervalFileIndex + 1) < sealedTsFiles.size()) {
                if (!singleTsFileReaderInitialized) {
                    IntervalFileNode fileNode = sealedTsFiles.get(++usedIntervalFileIndex);
                    //minTimestamp<=currentTimestamp<=maxTimestamp
                    if (singleTsFileSatisfied(fileNode)) {
                        initSingleTsFileReader(fileNode);
                        singleTsFileReaderInitialized = true;
                    }
                    else {
                        long minTimestamp = fileNode.getStartTime(path.getDeltaObjectToString());
                        long maxTimestamp = fileNode.getEndTime(path.getDeltaObjectToString());
                        if (maxTimestamp < currentTimestamp) {
                            continue;
                        }
                        else if (minTimestamp > currentTimestamp) {
                            return false;
                        }
                    }
                }
                //singleTsFileReader has already initialized
                TsPrimitiveType value = ((SeriesReaderFromSingleFileByTimestampImpl)singleTsFileReader).getValueInTimestamp(currentTimestamp);
                if(value != null){
                    hasCacheLastTimeValuePair = true;
                    cachedTimeValuePair = new TimeValuePair(currentTimestamp,value);
                    return true;
                }
                else {
                    singleTsFileReaderInitialized = false;
                }
            }
            return false;
        }

        @Override
        public TimeValuePair next() throws IOException {
            if(hasNext()){
                hasCacheLastTimeValuePair = false;
                return cachedTimeValuePair;
            }
            else {
                return null;
            }
        }

        protected boolean singleTsFileSatisfied(IntervalFileNode fileNode){

            if(fileNode.getStartTime(path.getDeltaObjectToString()) == -1){
                return false;
            }
            long minTime = fileNode.getStartTime(path.getDeltaObjectToString());
            long maxTime = fileNode.getEndTime(path.getDeltaObjectToString());
            return currentTimestamp >= minTime && currentTimestamp <= maxTime;
        }

        protected void initSingleTsFileReader(IntervalFileNode fileNode)throws IOException {
            ITsRandomAccessFileReader randomAccessFileReader = new TsRandomAccessLocalFileReader(fileNode.getFilePath());
            singleTsFileReader = new SeriesReaderFromSingleFileByTimestampImpl(randomAccessFileReader, path);
        }

        @Override
        public TsPrimitiveType getValueInTimestamp(long timestamp) throws IOException {
            currentTimestamp = timestamp;
            if(hasNext()){
                return next().getValue();
            }
            return null;
        }
    }

    protected class UnSealedTsFileWithTimeStampReader extends TsFilesReader.UnSealedTsFileReader implements SeriesReaderByTimeStamp{
        private boolean hasCacheLastTimeValuePair;
        private TimeValuePair cachedTimeValuePair;

        public UnSealedTsFileWithTimeStampReader(UnsealedTsFile unsealedTsFile) throws FileNotFoundException {
            super(unsealedTsFile);
            hasCacheLastTimeValuePair = false;
        }

        @Override
        public boolean hasNext() throws IOException {
            //hasCached
            if(hasCacheLastTimeValuePair && cachedTimeValuePair.getTimestamp() == currentTimestamp){
                return true;
            }

            TsPrimitiveType value = ((SeriesReaderFromSingleFileByTimestampImpl)singleTsFileReader).getValueInTimestamp(currentTimestamp);
            if(value != null){
                hasCacheLastTimeValuePair = true;
                cachedTimeValuePair = new TimeValuePair(currentTimestamp,value);
                return true;
            }
            return false;
        }
        @Override
        public TimeValuePair next() throws IOException {
            if(hasNext()){
                hasCacheLastTimeValuePair = false;
                return cachedTimeValuePair;
            }
            return null;
        }
        protected void initSingleTsFileReader(ITsRandomAccessFileReader randomAccessFileReader,
                                              SeriesChunkLoader seriesChunkLoader, List<EncodedSeriesChunkDescriptor> encodedSeriesChunkDescriptorList){
            singleTsFileReader = new SeriesReaderFromSingleFileByTimestampImpl(randomAccessFileReader, seriesChunkLoader, encodedSeriesChunkDescriptorList);
        }

        @Override
        public TsPrimitiveType getValueInTimestamp(long timestamp) throws IOException {
            if(hasNext()){
                return next().getValue();
            }
            return null;
        }
    }

}
