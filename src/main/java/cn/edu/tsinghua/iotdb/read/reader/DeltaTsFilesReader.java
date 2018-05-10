package cn.edu.tsinghua.iotdb.read.reader;

import cn.edu.tsinghua.iotdb.engine.filenode.IntervalFileNode;
import cn.edu.tsinghua.iotdb.engine.querycontext.GlobalSortedSeriesDataSource;
import cn.edu.tsinghua.iotdb.engine.querycontext.RawSeriesChunk;
import cn.edu.tsinghua.iotdb.engine.querycontext.UnsealedTsFile;
import cn.edu.tsinghua.iotdb.queryV2.engine.overflow.OverflowOperationReader;
import cn.edu.tsinghua.tsfile.common.utils.ITsRandomAccessFileReader;
import cn.edu.tsinghua.tsfile.file.metadata.TimeSeriesChunkMetaData;
import cn.edu.tsinghua.tsfile.timeseries.read.TsRandomAccessLocalFileReader;
import cn.edu.tsinghua.tsfile.timeseries.readV2.common.EncodedSeriesChunkDescriptor;
import cn.edu.tsinghua.tsfile.timeseries.readV2.controller.SeriesChunkLoader;
import cn.edu.tsinghua.tsfile.timeseries.readV2.controller.SeriesChunkLoaderImpl;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TimeValuePair;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.SeriesReader;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.impl.SeriesChunkReader;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DeltaTsFilesReader implements SeriesReader {

    private List<IntervalFileNode> sealedTsFiles;
    private UnsealedTsFile unsealedTsFile;
    private SeriesChunkReader unSealedSeriesChunkReader;
    private RawSeriesChunk rawSeriesChunk;

    private OverflowOperationReader overflowOperationReader;

    public DeltaTsFilesReader(GlobalSortedSeriesDataSource sortedSeriesDataSource, OverflowOperationReader overflowOperationReader)
            throws FileNotFoundException {
        this.sealedTsFiles = sortedSeriesDataSource.getSealedTsFiles();
        this.unsealedTsFile = sortedSeriesDataSource.getUnsealedTsFile();
        this.rawSeriesChunk = sortedSeriesDataSource.getRawSeriesChunk();

        // add unsealed file TimeSeriesChunkMetadata
        List<EncodedSeriesChunkDescriptor> encodedSeriesChunkDescriptorList = new ArrayList<>();
        for (TimeSeriesChunkMetaData timeSeriesChunkMetaData : sortedSeriesDataSource.getUnsealedTsFile().getTimeSeriesChunkMetaDatas()) {
            encodedSeriesChunkDescriptorList.add(generateSeriesChunkDescriptorByMetadata(timeSeriesChunkMetaData, unsealedTsFile.getFilePath()));
        }

        // TODO unSealedSeriesChunkReader need to be constructed correctly
        ITsRandomAccessFileReader randomAccessFileReader = new TsRandomAccessLocalFileReader(unsealedTsFile.getFilePath());
        SeriesChunkLoader seriesChunkLoader = new SeriesChunkLoaderImpl(randomAccessFileReader);

        this.overflowOperationReader = overflowOperationReader;
    }

    @Override
    public boolean hasNext() throws IOException {
        return false;
    }

    @Override
    public TimeValuePair next() throws IOException {
        return null;
    }

    @Override
    public void skipCurrentTimeValuePair() throws IOException {

    }

    @Override
    public void close() throws IOException {

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
}
