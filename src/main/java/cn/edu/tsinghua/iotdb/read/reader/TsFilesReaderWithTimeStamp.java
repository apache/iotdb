package cn.edu.tsinghua.iotdb.read.reader;

import cn.edu.tsinghua.iotdb.engine.filenode.IntervalFileNode;
import cn.edu.tsinghua.iotdb.engine.querycontext.GlobalSortedSeriesDataSource;
import cn.edu.tsinghua.iotdb.engine.querycontext.UnsealedTsFile;
import cn.edu.tsinghua.iotdb.queryV2.engine.reader.PriorityMergeSortTimeValuePairReader;
import cn.edu.tsinghua.iotdb.queryV2.engine.reader.PriorityTimeValuePairReader;
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
import java.util.ArrayList;
import java.util.List;

public class TsFilesReaderWithTimeStamp extends TsFilesReader implements SeriesReaderByTimeStamp {

    private long currentTimestamp;
    private RawSeriesChunkReaderByTimestamp rawSeriesChunkReaderByTimestamp;

    public TsFilesReaderWithTimeStamp(GlobalSortedSeriesDataSource sortedSeriesDataSource)
            throws IOException {
        super(sortedSeriesDataSource);

        List<PriorityTimeValuePairReader> timeValuePairReaders = new ArrayList<>();
        int priorityValue = 1;

        //data in sealedTsFiles and unSealedTsFile
        if(sortedSeriesDataSource.getSealedTsFiles() != null){
            TsFilesReaderWithTimeStamp.SealedTsFileWithTimeStampReader sealedTsFileWithTimeStampReader = new TsFilesReaderWithTimeStamp.SealedTsFileWithTimeStampReader(sortedSeriesDataSource.getSealedTsFiles());
            timeValuePairReaders.add(new PriorityTimeValuePairReader(sealedTsFileWithTimeStampReader, new PriorityTimeValuePairReader.Priority(priorityValue++)));
        }
        if(sortedSeriesDataSource.getUnsealedTsFile() != null){
            TsFilesReaderWithTimeStamp.UnSealedTsFileWithTimeStampReader unSealedTsFileWithTimeStampReader = new TsFilesReaderWithTimeStamp.UnSealedTsFileWithTimeStampReader(sortedSeriesDataSource.getUnsealedTsFile());
            timeValuePairReaders.add(new PriorityTimeValuePairReader(unSealedTsFileWithTimeStampReader, new PriorityTimeValuePairReader.Priority(priorityValue++)));
        }



        //data in memTable
        if(sortedSeriesDataSource.hasRawSeriesChunk()) {
            rawSeriesChunkReaderByTimestamp = new RawSeriesChunkReaderByTimestamp(sortedSeriesDataSource.getRawSeriesChunk());
        }

        this.seriesReader = new PriorityMergeSortTimeValuePairReader(timeValuePairReaders);
    }

    @Override
    public boolean hasNext()  throws IOException {
        return seriesReader.hasNext() || rawSeriesChunkReaderByTimestamp.hasNext();
    }

    @Override
    public TimeValuePair next() throws IOException {
        if(rawSeriesChunkReaderByTimestamp.hasNext()){
            return rawSeriesChunkReaderByTimestamp.next();
        }
        if(seriesReader.hasNext()){
            return seriesReader.next();
        }
        return null;
    }

    @Override
    public void close() throws IOException {
        seriesReader.close();
        rawSeriesChunkReaderByTimestamp.close();
    }

    /**
     * @param timestamp
     * @return If there is no TimeValuePair whose timestamp equals to given timestamp, then return null.
     * @throws IOException
     */
    public TsPrimitiveType getValueInTimestamp(long timestamp) throws IOException {
        setCurrentTimestamp(timestamp);
        if(rawSeriesChunkReaderByTimestamp.hasNext()){
            return rawSeriesChunkReaderByTimestamp.next().getValue();
        }
        if(seriesReader.hasNext()){
            return seriesReader.next().getValue();
        }
        return null;
    }

    @Override
    public void setCurrentTimestamp(long currentTimestamp) {
        this.currentTimestamp = currentTimestamp;
        rawSeriesChunkReaderByTimestamp.setCurrentTimestamp(currentTimestamp);
    }

    protected class SealedTsFileWithTimeStampReader extends TsFilesReader.SealedTsFileReader{

        public SealedTsFileWithTimeStampReader(List<IntervalFileNode> sealedTsFiles){
            super(sealedTsFiles);
        }

        @Override
        public boolean hasNext() throws IOException {
            if (singleTsFileReaderInitialized) {
                ((SeriesReaderFromSingleFileByTimestampImpl)singleTsFileReader).setCurrentTimestamp(currentTimestamp);
                if(singleTsFileReader.hasNext()) {
                    return true;
                }
            }
            while ((usedIntervalFileIndex + 1)< sealedTsFiles.size()) {
                if (!singleTsFileReaderInitialized) {
                    IntervalFileNode fileNode = sealedTsFiles.get(++usedIntervalFileIndex);
                    if (singleTsFileSatisfied(fileNode)) {
                        initSingleTsFileReader(fileNode);
                        ((SeriesReaderFromSingleFileByTimestampImpl)singleTsFileReader).setCurrentTimestamp(currentTimestamp);
                        singleTsFileReaderInitialized = true;
                        usedIntervalFileIndex++;
                    } else {
                        long minTimestamp = fileNode.getStartTime(path.getDeltaObjectToString());
                        long maxTimestamp = fileNode.getEndTime(path.getDeltaObjectToString());
                        if (maxTimestamp < currentTimestamp) {
                            continue;
                        } else if (minTimestamp > currentTimestamp) {
                            return false;
                        }
                    }
                }
                if (singleTsFileReader.hasNext()) {
                    return true;
                } else {
                    singleTsFileReaderInitialized = false;
                }
            }
            return false;
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

    }

    protected class UnSealedTsFileWithTimeStampReader extends TsFilesReader.UnSealedTsFileReader{
        public UnSealedTsFileWithTimeStampReader(UnsealedTsFile unsealedTsFile) throws FileNotFoundException {
            super(unsealedTsFile);
        }

        @Override
        public boolean hasNext() throws IOException {
            ((SeriesReaderFromSingleFileByTimestampImpl)singleTsFileReader).setCurrentTimestamp(currentTimestamp);
            return singleTsFileReader.hasNext();
        }

        protected void initSingleTsFileReader(ITsRandomAccessFileReader randomAccessFileReader,
                                              SeriesChunkLoader seriesChunkLoader, List<EncodedSeriesChunkDescriptor> encodedSeriesChunkDescriptorList){
            singleTsFileReader = new SeriesReaderFromSingleFileByTimestampImpl(randomAccessFileReader, seriesChunkLoader, encodedSeriesChunkDescriptorList);
        }
    }

}
