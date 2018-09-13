package cn.edu.tsinghua.iotdb.read.reader;

import cn.edu.tsinghua.iotdb.engine.filenode.IntervalFileNode;
import cn.edu.tsinghua.iotdb.engine.querycontext.GlobalSortedSeriesDataSource;
import cn.edu.tsinghua.iotdb.engine.querycontext.UnsealedTsFile;
import cn.edu.tsinghua.iotdb.queryV2.engine.control.OverflowFileStreamManager;
import cn.edu.tsinghua.iotdb.queryV2.engine.control.QueryJobManager;
import cn.edu.tsinghua.tsfile.common.utils.ITsRandomAccessFileReader;
import cn.edu.tsinghua.tsfile.file.metadata.TimeSeriesChunkMetaData;
import cn.edu.tsinghua.tsfile.timeseries.read.TsRandomAccessLocalFileReader;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import cn.edu.tsinghua.tsfile.timeseries.readV2.common.EncodedSeriesChunkDescriptor;
import cn.edu.tsinghua.tsfile.timeseries.readV2.controller.SeriesChunkLoader;
import cn.edu.tsinghua.tsfile.timeseries.readV2.controller.SeriesChunkLoaderImpl;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TimeValuePair;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.SeriesReader;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

/**
 * A reader for sequentially inserts data，including a list of sealedTsFile, unSealedTsFile, data in MemTable.
 * */
public abstract class SequenceInsertDataReader implements SeriesReader {

    protected List<SeriesReader> seriesReaders;
    protected Path path;
    protected long jobId;

    private boolean hasSeriesReaderInitialized;
    private int nextSeriesReaderIndex;
    private SeriesReader currentSeriesReader;

    public SequenceInsertDataReader(GlobalSortedSeriesDataSource sortedSeriesDataSource){
        path = sortedSeriesDataSource.getSeriesPath();
        seriesReaders = new ArrayList<SeriesReader>();
        jobId = QueryJobManager.getInstance().addJobForOneQuery();

        hasSeriesReaderInitialized = false;
        nextSeriesReaderIndex = 0;
    }

    @Override
    public boolean hasNext() throws IOException {
        if(hasSeriesReaderInitialized && currentSeriesReader.hasNext()){
            return true;
        }
        else {
            hasSeriesReaderInitialized = false;
        }

        while (nextSeriesReaderIndex < seriesReaders.size()){
            if(!hasSeriesReaderInitialized){
                currentSeriesReader = seriesReaders.get(nextSeriesReaderIndex++);
                hasSeriesReaderInitialized = true;
            }
            if(currentSeriesReader.hasNext()){
                return true;
            }
            else {
                hasSeriesReaderInitialized = false;
            }
        }
        return false;
    }

    @Override
    public TimeValuePair next() throws IOException {
        return currentSeriesReader.next();
    }

    @Override
    public void skipCurrentTimeValuePair() throws IOException {
        next();
    }

    @Override
    public void close() throws IOException {
        for (SeriesReader seriesReader: seriesReaders){
            seriesReader.close();
        }
    }

    private EncodedSeriesChunkDescriptor generateSeriesChunkDescriptorByMetadata(TimeSeriesChunkMetaData timeSeriesChunkMetaData, String filePath) {
        EncodedSeriesChunkDescriptor encodedSeriesChunkDescriptor = new EncodedSeriesChunkDescriptor(filePath,
                timeSeriesChunkMetaData.getProperties().getFileOffset(),
                timeSeriesChunkMetaData.getTotalByteSize(),
                timeSeriesChunkMetaData.getProperties().getCompression(),
                timeSeriesChunkMetaData.getVInTimeSeriesChunkMetaData().getDataType(),
                timeSeriesChunkMetaData.getVInTimeSeriesChunkMetaData().getDigest(),
                timeSeriesChunkMetaData.getTInTimeSeriesChunkMetaData().getStartTime(),
                timeSeriesChunkMetaData.getTInTimeSeriesChunkMetaData().getEndTime(),
                timeSeriesChunkMetaData.getNumRows(),
                timeSeriesChunkMetaData.getVInTimeSeriesChunkMetaData().getEnumValues());
        return encodedSeriesChunkDescriptor;
    }

    protected abstract class SealedTsFileReader implements SeriesReader{

        protected List<IntervalFileNode> sealedTsFiles;
        protected int usedIntervalFileIndex;
        protected SeriesReader singleTsFileReader;
        protected boolean singleTsFileReaderInitialized;

        public SealedTsFileReader(List<IntervalFileNode> sealedTsFiles) {
            this.sealedTsFiles = sealedTsFiles;
            this.usedIntervalFileIndex = -1;
            this.singleTsFileReader = null;
            this.singleTsFileReaderInitialized = false;
        }

        @Override
        public boolean hasNext() throws IOException {
            if(singleTsFileReaderInitialized && singleTsFileReader.hasNext()){
                return true;
            }
            while ((usedIntervalFileIndex + 1) < sealedTsFiles.size()){
                if(!singleTsFileReaderInitialized){
                    IntervalFileNode fileNode = sealedTsFiles.get(++usedIntervalFileIndex);
                    if(singleTsFileSatisfied(fileNode)) {
                        initSingleTsFileReader(fileNode);
                        singleTsFileReaderInitialized = true;
                    }
                    else {
                        continue;
                    }
                }
                if(singleTsFileReader.hasNext()){
                    return true;
                }
                else{
                    singleTsFileReaderInitialized = false;
                }
            }
            return false;
        }

        @Override
        public TimeValuePair next() throws IOException {
            return singleTsFileReader.next();
        }

        @Override
        public void skipCurrentTimeValuePair() throws IOException {
            next();
        }

        @Override
        public void close() throws IOException {
            if (singleTsFileReader != null) {
                singleTsFileReader.close();
            }
        }

        protected abstract boolean singleTsFileSatisfied(IntervalFileNode fileNode);

        protected abstract void initSingleTsFileReader(IntervalFileNode fileNode) throws IOException;
    }

    protected abstract class UnSealedTsFileReader implements SeriesReader{
        protected UnsealedTsFile unsealedTsFile;
        protected SeriesReader singleTsFileReader;


        public UnSealedTsFileReader(UnsealedTsFile unsealedTsFile) throws IOException {
            this.unsealedTsFile = unsealedTsFile;

            // add unsealed file TimeSeriesChunkMetadata
            List<EncodedSeriesChunkDescriptor> encodedSeriesChunkDescriptorList = new ArrayList<>();
            for (TimeSeriesChunkMetaData timeSeriesChunkMetaData : unsealedTsFile.getTimeSeriesChunkMetaDatas()) {
                encodedSeriesChunkDescriptorList.add(generateSeriesChunkDescriptorByMetadata(timeSeriesChunkMetaData, unsealedTsFile.getFilePath()));
            }

            // TODO unSealedSeriesChunkReader need to be constructed correctly
            RandomAccessFile raf = OverflowFileStreamManager.getInstance().get(jobId, unsealedTsFile.getFilePath());
            ITsRandomAccessFileReader randomAccessFileReader = new TsRandomAccessLocalFileReader(raf);
            SeriesChunkLoader seriesChunkLoader = new SeriesChunkLoaderImpl(randomAccessFileReader);

            initSingleTsFileReader(randomAccessFileReader, seriesChunkLoader, encodedSeriesChunkDescriptorList);
        }

        @Override
        public boolean hasNext() throws IOException {
            return singleTsFileReader.hasNext();
        }

        @Override
        public TimeValuePair next() throws IOException {
            return singleTsFileReader.next();
        }

        @Override
        public void skipCurrentTimeValuePair() throws IOException {
            singleTsFileReader.skipCurrentTimeValuePair();
        }

        @Override
        public void close() throws IOException {
            if(singleTsFileReader!=null){
                singleTsFileReader.close();
            }
        }

        protected abstract void initSingleTsFileReader(ITsRandomAccessFileReader randomAccessFileReader, SeriesChunkLoader seriesChunkLoader, List<EncodedSeriesChunkDescriptor> encodedSeriesChunkDescriptorList);
    }
}
