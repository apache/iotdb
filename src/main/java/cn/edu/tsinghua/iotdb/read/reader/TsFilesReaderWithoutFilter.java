package cn.edu.tsinghua.iotdb.read.reader;

import cn.edu.tsinghua.iotdb.engine.filenode.IntervalFileNode;
import cn.edu.tsinghua.iotdb.engine.querycontext.GlobalSortedSeriesDataSource;
import cn.edu.tsinghua.iotdb.engine.querycontext.UnsealedTsFile;
import cn.edu.tsinghua.iotdb.queryV2.engine.reader.PriorityMergeSortTimeValuePairReader;
import cn.edu.tsinghua.iotdb.queryV2.engine.reader.PriorityTimeValuePairReader;
import cn.edu.tsinghua.iotdb.queryV2.engine.reader.series.RawSeriesChunkReaderWithoutFilter;
import cn.edu.tsinghua.tsfile.common.utils.ITsRandomAccessFileReader;
import cn.edu.tsinghua.tsfile.timeseries.read.TsRandomAccessLocalFileReader;
import cn.edu.tsinghua.tsfile.timeseries.readV2.common.EncodedSeriesChunkDescriptor;
import cn.edu.tsinghua.tsfile.timeseries.readV2.controller.SeriesChunkLoader;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.impl.SeriesReaderFromSingleFileWithoutFilterImpl;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
public class TsFilesReaderWithoutFilter extends TsFilesReader{

    public TsFilesReaderWithoutFilter(GlobalSortedSeriesDataSource sortedSeriesDataSource)
            throws IOException {
        super(sortedSeriesDataSource);

        List<PriorityTimeValuePairReader> timeValuePairReaders = new ArrayList<>();
        int priorityValue = 1;

        //add data in sealedTsFiles and unSealedTsFile
        SealedTsFileWithoutFilterReader sealedTsFileWithoutFilterReader = new SealedTsFileWithoutFilterReader(sortedSeriesDataSource.getSealedTsFiles());
        UnSealedTsFileWithoutFilterReader unSealedTsFileWithoutFilterReader = new UnSealedTsFileWithoutFilterReader(sortedSeriesDataSource.getUnsealedTsFile());
        timeValuePairReaders.add(new PriorityTimeValuePairReader(sealedTsFileWithoutFilterReader, new PriorityTimeValuePairReader.Priority(priorityValue++)));
        timeValuePairReaders.add(new PriorityTimeValuePairReader(unSealedTsFileWithoutFilterReader, new PriorityTimeValuePairReader.Priority(priorityValue++)));

        //add data in memTable
        if(sortedSeriesDataSource.hasRawSeriesChunk()) {
            timeValuePairReaders.add(new PriorityTimeValuePairReader(new RawSeriesChunkReaderWithoutFilter(
                    sortedSeriesDataSource.getRawSeriesChunk()), new PriorityTimeValuePairReader.Priority(priorityValue++)));
        }

        this.seriesReader = new PriorityMergeSortTimeValuePairReader(timeValuePairReaders);
    }

    protected class SealedTsFileWithoutFilterReader extends TsFilesReader.SealedTsFileReader{
        public SealedTsFileWithoutFilterReader(List<IntervalFileNode> sealedTsFiles){
            super(sealedTsFiles);
        }

        protected boolean singleTsFileSatisfied(IntervalFileNode fileNode){
            return true;
        }

        protected void initSingleTsFileReader(IntervalFileNode fileNode)throws IOException{
            ITsRandomAccessFileReader randomAccessFileReader = new TsRandomAccessLocalFileReader(fileNode.getFilePath());
            singleTsFileReader = new SeriesReaderFromSingleFileWithoutFilterImpl(randomAccessFileReader, path);
        }
    }

    protected class UnSealedTsFileWithoutFilterReader extends TsFilesReader.UnSealedTsFileReader{
        public UnSealedTsFileWithoutFilterReader(UnsealedTsFile unsealedTsFile) throws FileNotFoundException{
            super(unsealedTsFile);
        }

        protected void initSingleTsFileReader(ITsRandomAccessFileReader randomAccessFileReader,
                                                          SeriesChunkLoader seriesChunkLoader, List<EncodedSeriesChunkDescriptor> encodedSeriesChunkDescriptorList){
            singleTsFileReader = new SeriesReaderFromSingleFileWithoutFilterImpl(randomAccessFileReader, seriesChunkLoader, encodedSeriesChunkDescriptorList);
        }
    }
}
