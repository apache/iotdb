package cn.edu.tsinghua.iotdb.read.reader;

import cn.edu.tsinghua.iotdb.engine.filenode.IntervalFileNode;
import cn.edu.tsinghua.iotdb.engine.querycontext.GlobalSortedSeriesDataSource;
import cn.edu.tsinghua.iotdb.engine.querycontext.UnsealedTsFile;
import cn.edu.tsinghua.iotdb.queryV2.engine.reader.PriorityMergeSortTimeValuePairReader;
import cn.edu.tsinghua.iotdb.queryV2.engine.reader.PriorityTimeValuePairReader;
import cn.edu.tsinghua.iotdb.queryV2.engine.reader.series.RawSeriesChunkReaderWithFilter;
import cn.edu.tsinghua.tsfile.common.utils.ITsRandomAccessFileReader;
import cn.edu.tsinghua.tsfile.timeseries.filter.utils.DigestForFilter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.QueryFilterType;
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
        //add data in sealedTsFiles and unSealedTsFile
        if(sortedSeriesDataSource.getSealedTsFiles() != null){
            seriesReaders.add(new TsFilesReaderWithFilter.SealedTsFileWithFilterReader(sortedSeriesDataSource.getSealedTsFiles()));
        }
        if(sortedSeriesDataSource.getUnsealedTsFile() != null){
            seriesReaders.add(new TsFilesReaderWithFilter.UnSealedTsFileWithFilterReader(sortedSeriesDataSource.getUnsealedTsFile()));
        }

        //add data in memTable
        if(sortedSeriesDataSource.hasRawSeriesChunk()) {
            seriesReaders.add(new RawSeriesChunkReaderWithFilter(sortedSeriesDataSource.getRawSeriesChunk(), filter.getFilter()));
        }
    }

    protected class SealedTsFileWithFilterReader extends TsFilesReader.SealedTsFileReader{


        public SealedTsFileWithFilterReader(List<IntervalFileNode> sealedTsFiles){
            super(sealedTsFiles);
        }

        protected boolean singleTsFileSatisfied(IntervalFileNode fileNode){
            if(fileNode.getStartTime(path.getDeltaObjectToString()) == -1){
                return false;
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
