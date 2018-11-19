package cn.edu.tsinghua.iotdb.read.reader;

import cn.edu.tsinghua.iotdb.engine.filenode.IntervalFileNode;
import cn.edu.tsinghua.iotdb.engine.querycontext.GlobalSortedSeriesDataSource;
import cn.edu.tsinghua.iotdb.engine.querycontext.UnsealedTsFile;
import cn.edu.tsinghua.iotdb.queryV2.engine.control.OverflowFileStreamManager;
import cn.edu.tsinghua.iotdb.queryV2.engine.reader.series.RawSeriesChunkReaderWithFilter;
import cn.edu.tsinghua.iotdb.queryV2.engine.reader.series.RawSeriesChunkReaderWithoutFilter;
import cn.edu.tsinghua.tsfile.common.utils.ITsRandomAccessFileReader;
import cn.edu.tsinghua.tsfile.timeseries.filter.utils.DigestForFilter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.QueryFilterType;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.impl.SeriesFilter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.visitor.impl.DigestFilterVisitor;
import cn.edu.tsinghua.tsfile.timeseries.read.TsRandomAccessLocalFileReader;
import cn.edu.tsinghua.tsfile.timeseries.readV2.common.EncodedSeriesChunkDescriptor;
import cn.edu.tsinghua.tsfile.timeseries.readV2.controller.SeriesChunkLoader;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.impl.SeriesReaderFromSingleFileWithFilterImpl;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.impl.SeriesReaderFromSingleFileWithoutFilterImpl;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.List;

/***/
public class SequenceInsertDataWithOrWithOutFilterReader extends SequenceInsertDataReader {
    private SeriesFilter<?> filter;

    public SequenceInsertDataWithOrWithOutFilterReader(GlobalSortedSeriesDataSource sortedSeriesDataSource, SeriesFilter<?> filter)
            throws IOException {
        super(sortedSeriesDataSource);

        this.filter = filter;
        //add data in sealedTsFiles and unSealedTsFile
        if(sortedSeriesDataSource.getSealedTsFiles() != null){
            seriesReaders.add(new SequenceInsertDataWithOrWithOutFilterReader.SealedTsFileWithFilterReader(sortedSeriesDataSource.getSealedTsFiles()));
        }
        if(sortedSeriesDataSource.getUnsealedTsFile() != null){
            seriesReaders.add(new SequenceInsertDataWithOrWithOutFilterReader.UnSealedTsFileWithFilterReader(sortedSeriesDataSource.getUnsealedTsFile()));
        }

        //add data in memTable
        if(sortedSeriesDataSource.hasRawSeriesChunk() && filter == null) {
            seriesReaders.add(new RawSeriesChunkReaderWithoutFilter(sortedSeriesDataSource.getRawSeriesChunk()));
        }
        if(sortedSeriesDataSource.hasRawSeriesChunk() && filter != null) {
            seriesReaders.add(new RawSeriesChunkReaderWithFilter(sortedSeriesDataSource.getRawSeriesChunk(), filter.getFilter()));
        }
    }

    protected class SealedTsFileWithFilterReader extends SequenceInsertDataReader.SealedTsFileReader{


        public SealedTsFileWithFilterReader(List<IntervalFileNode> sealedTsFiles){
            super(sealedTsFiles);
        }

        protected boolean singleTsFileSatisfied(IntervalFileNode fileNode){
            if(fileNode.getStartTime(path.getDeltaObjectToString()) == -1){
                return false;
            }
            //no filter
            if(filter == null){
                return true;
            }

            if(filter.getType() == QueryFilterType.GLOBAL_TIME){//filter time
                DigestFilterVisitor digestFilterVisitor = new DigestFilterVisitor();
                DigestForFilter timeDigest = new DigestForFilter(fileNode.getStartTime(path.getDeltaObjectToString()),
                        fileNode.getEndTime(path.getDeltaObjectToString()));
                return digestFilterVisitor.satisfy(timeDigest, null, filter.getFilter());
            }
            else{//fileNode doesn't hold the value scope for series
                return true;
            }
        }

        protected void initSingleTsFileReader(IntervalFileNode fileNode)throws IOException {
            RandomAccessFile raf = OverflowFileStreamManager.getInstance().get(jobId, fileNode.getFilePath());
            ITsRandomAccessFileReader randomAccessFileReader = new TsRandomAccessLocalFileReader(raf);

            if(filter == null){
                singleTsFileReader = new SeriesReaderFromSingleFileWithoutFilterImpl(randomAccessFileReader, path);
            }
            else{
                singleTsFileReader = new SeriesReaderFromSingleFileWithFilterImpl(randomAccessFileReader, path, filter.getFilter());
            }

        }
    }

    protected class UnSealedTsFileWithFilterReader extends SequenceInsertDataReader.UnSealedTsFileReader{
        public UnSealedTsFileWithFilterReader(UnsealedTsFile unsealedTsFile) throws IOException {
            super(unsealedTsFile);
        }

        protected void initSingleTsFileReader(ITsRandomAccessFileReader randomAccessFileReader,
                                              SeriesChunkLoader seriesChunkLoader, List<EncodedSeriesChunkDescriptor> encodedSeriesChunkDescriptorList){
            if(filter == null){
                singleTsFileReader = new SeriesReaderFromSingleFileWithoutFilterImpl(randomAccessFileReader, seriesChunkLoader, encodedSeriesChunkDescriptorList);
            }
            else{
                singleTsFileReader = new SeriesReaderFromSingleFileWithFilterImpl(randomAccessFileReader, seriesChunkLoader, encodedSeriesChunkDescriptorList, filter.getFilter());
            }
        }
    }
}
