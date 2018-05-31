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

        //add data in sealedTsFiles and unSealedTsFile
        if(sortedSeriesDataSource.getSealedTsFiles() != null){
            seriesReaders.add(new SealedTsFileWithoutFilterReader(sortedSeriesDataSource.getSealedTsFiles()));
        }
        if(sortedSeriesDataSource.getUnsealedTsFile() != null){
            seriesReaders.add(new UnSealedTsFileWithoutFilterReader(sortedSeriesDataSource.getUnsealedTsFile()));
        }

        //add data in memTable
        if(sortedSeriesDataSource.hasRawSeriesChunk()) {
            seriesReaders.add(new RawSeriesChunkReaderWithoutFilter(sortedSeriesDataSource.getRawSeriesChunk()));
        }
    }

    protected class SealedTsFileWithoutFilterReader extends TsFilesReader.SealedTsFileReader{
        public SealedTsFileWithoutFilterReader(List<IntervalFileNode> sealedTsFiles){
            super(sealedTsFiles);
        }

        protected boolean singleTsFileSatisfied(IntervalFileNode fileNode){
            //check if this filenode contains the record of this deltaobject
            return fileNode.getStartTime(path.getDeltaObjectToString()) != -1;
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
