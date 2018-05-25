package cn.edu.tsinghua.iotdb.read.reader;

import cn.edu.tsinghua.iotdb.engine.filenode.IntervalFileNode;
import cn.edu.tsinghua.iotdb.engine.querycontext.GlobalSortedSeriesDataSource;
import cn.edu.tsinghua.iotdb.engine.querycontext.UnsealedTsFile;
import cn.edu.tsinghua.iotdb.queryV2.engine.reader.PriorityMergeSortTimeValuePairReader;
import cn.edu.tsinghua.iotdb.queryV2.engine.reader.PriorityTimeValuePairReader;
import cn.edu.tsinghua.iotdb.queryV2.engine.reader.series.RawSeriesChunkReaderWithFilter;
import cn.edu.tsinghua.tsfile.common.utils.ITsRandomAccessFileReader;
import cn.edu.tsinghua.tsfile.timeseries.filter.utils.DigestForFilter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.basic.Filter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.QueryFilterType;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.impl.GlobalTimeFilter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.impl.SeriesFilter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.visitor.impl.DigestFilterVisitor;
import cn.edu.tsinghua.tsfile.timeseries.read.TsRandomAccessLocalFileReader;
import cn.edu.tsinghua.tsfile.timeseries.readV2.common.EncodedSeriesChunkDescriptor;
import cn.edu.tsinghua.tsfile.timeseries.readV2.controller.SeriesChunkLoader;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.impl.SeriesReaderFromSingleFileWithFilterImpl;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/***/
public class TsFilesReaderWithFilter extends TsFilesReader {
    private SeriesFilter<?> filter;

    public TsFilesReaderWithFilter(GlobalSortedSeriesDataSource sortedSeriesDataSource, SeriesFilter<?> filter)
            throws IOException {
        super(sortedSeriesDataSource);

        this.filter = filter;

        List<PriorityTimeValuePairReader> timeValuePairReaders = new ArrayList<>();
        int priorityValue = 1;

        //add data in sealedTsFiles and unSealedTsFile
        TsFilesReaderWithFilter.SealedTsFileWithFilterReader sealedTsFileWithFilterReader = new TsFilesReaderWithFilter.SealedTsFileWithFilterReader(sortedSeriesDataSource.getSealedTsFiles());
        TsFilesReaderWithFilter.UnSealedTsFileWithFilterReader unSealedTsFileWithFilterReader = new TsFilesReaderWithFilter.UnSealedTsFileWithFilterReader(sortedSeriesDataSource.getUnsealedTsFile());
        timeValuePairReaders.add(new PriorityTimeValuePairReader(sealedTsFileWithFilterReader, new PriorityTimeValuePairReader.Priority(priorityValue++)));
        timeValuePairReaders.add(new PriorityTimeValuePairReader(unSealedTsFileWithFilterReader, new PriorityTimeValuePairReader.Priority(priorityValue++)));

        //add data in memTable
        if(sortedSeriesDataSource.hasRawSeriesChunk()) {
            timeValuePairReaders.add(new PriorityTimeValuePairReader(new RawSeriesChunkReaderWithFilter(
                    sortedSeriesDataSource.getRawSeriesChunk(), filter.getFilter()), new PriorityTimeValuePairReader.Priority(priorityValue++)));
        }

        this.seriesReader = new PriorityMergeSortTimeValuePairReader(timeValuePairReaders);
    }

    protected class SealedTsFileWithFilterReader extends TsFilesReader.SealedTsFileReader{


        public SealedTsFileWithFilterReader(List<IntervalFileNode> sealedTsFiles){
            super(sealedTsFiles);
        }

        protected boolean singleTsFileSatisfied(IntervalFileNode fileNode){

            if(filter.getType() == QueryFilterType.GLOBAL_TIME){//filter time
                DigestFilterVisitor digestFilterVisitor = new DigestFilterVisitor();
                DigestForFilter timeDigest = new DigestForFilter(fileNode.getStartTime(path.getDeltaObjectToString()),
                        fileNode.getStartTime(path.getDeltaObjectToString()));
                return digestFilterVisitor.satisfy(timeDigest, null, filter.getFilter());
            }
            else{//fileNode doesn't hold the value scope for series
                return true;
            }
        }

        protected void initSingleTsFileReader(IntervalFileNode fileNode)throws IOException {
            ITsRandomAccessFileReader randomAccessFileReader = new TsRandomAccessLocalFileReader(fileNode.getFilePath());
            singleTsFileReader = new SeriesReaderFromSingleFileWithFilterImpl(randomAccessFileReader, path, filter.getFilter());
        }
    }

    protected class UnSealedTsFileWithFilterReader extends TsFilesReader.UnSealedTsFileReader{
        public UnSealedTsFileWithFilterReader(UnsealedTsFile unsealedTsFile) throws FileNotFoundException {
            super(unsealedTsFile);
        }

        protected void initSingleTsFileReader(ITsRandomAccessFileReader randomAccessFileReader,
                                              SeriesChunkLoader seriesChunkLoader, List<EncodedSeriesChunkDescriptor> encodedSeriesChunkDescriptorList){
            singleTsFileReader = new SeriesReaderFromSingleFileWithFilterImpl(randomAccessFileReader, seriesChunkLoader, encodedSeriesChunkDescriptorList, filter.getFilter());
        }
    }
}
