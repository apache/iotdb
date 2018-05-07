package cn.edu.tsinghua.iotdb.query.reader;

import cn.edu.tsinghua.iotdb.engine.filenode.IntervalFileNode;
import cn.edu.tsinghua.iotdb.engine.querycontext.GlobalSortedSeriesDataSource;
import cn.edu.tsinghua.iotdb.engine.querycontext.OverflowSeriesDataSource;
import cn.edu.tsinghua.iotdb.engine.querycontext.RawSeriesChunk;
import cn.edu.tsinghua.iotdb.exception.PathErrorException;
import cn.edu.tsinghua.iotdb.metadata.MManager;
import cn.edu.tsinghua.iotdb.query.management.FileReaderMap;
import cn.edu.tsinghua.iotdb.query.management.ReaderManager;
import cn.edu.tsinghua.iotdb.queryV2.engine.overflow.OverflowOperationReader;
import cn.edu.tsinghua.iotdb.queryV2.engine.reader.series.OverflowInsertDataReader;
import cn.edu.tsinghua.iotdb.queryV2.factory.SeriesReaderFactory;
import cn.edu.tsinghua.tsfile.file.metadata.TimeSeriesChunkMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.enums.CompressionTypeName;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.utils.Interval;
import cn.edu.tsinghua.tsfile.timeseries.filter.visitorImpl.SingleValueVisitor;
import cn.edu.tsinghua.tsfile.timeseries.read.TsRandomAccessLocalFileReader;
import cn.edu.tsinghua.tsfile.timeseries.read.ValueReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static cn.edu.tsinghua.iotdb.query.reader.ReaderUtils.getSingleValueVisitorByDataType;

/**
 * <p>
 * A <code>RecordReader</code> contains all the data variables which is needed in read process.
 * Note that : it only contains the data of a (deltaObjectId, measurementId).
 * </p>
 */
public class RecordReader {

    static final Logger logger = LoggerFactory.getLogger(RecordReader.class);

    protected String deltaObjectId, measurementId;

    /** data type **/
    protected TSDataType dataType;

    /** compression type in this series **/
    public CompressionTypeName compressionTypeName;

    /** TsFile ReaderManager for current (deltaObjectId, measurementId) **/
    protected ReaderManager tsFileReaderManager;

    /** unsealed file **/
    protected List<ValueReader> valueReaders;

    /** memtable data in memory **/
    protected RawSeriesChunk memRawSeriesChunk;

    public OverflowInsertDataReader getOverflowSeriesInsertReader() {
        return overflowSeriesInsertReader;
    }

    public OverflowOperationReader getOverflowOperationReader() {
        return overflowOperationReader;
    }

    /** overflow insert data reader **/
    protected OverflowInsertDataReader overflowSeriesInsertReader;

    /** overflow update data reader **/
    protected OverflowOperationReader overflowOperationReader;
    protected OverflowOperationReader overflowOperationReaderCopy;

    /** series time filter, this filter is the filter **/
    protected SingleSeriesFilterExpression queryTimeFilter;
    protected SingleValueVisitor<?> singleTimeVisitor;

    /** series value filter **/
    protected SingleSeriesFilterExpression queryValueFilter;
    protected SingleValueVisitor<?> singleValueVisitor;

    /** memRawSeriesChunk + overflowSeriesInsertReader + overflowOperationReader **/
    protected InsertDynamicData insertMemoryData;

    public RecordReader(GlobalSortedSeriesDataSource globalSortedSeriesDataSource, OverflowSeriesDataSource overflowSeriesDataSource,
                        String deltaObjectId, String measurementId,
                        SingleSeriesFilterExpression queryTimeFilter, SingleSeriesFilterExpression queryValueFilter)
            throws PathErrorException, IOException {

        List<String> sealedFilePathList = new ArrayList<>();
        for (IntervalFileNode fileNode : globalSortedSeriesDataSource.getSealedTsFiles()) {
            sealedFilePathList.add(fileNode.getFilePath());
        }
        this.tsFileReaderManager = new ReaderManager(sealedFilePathList);

        valueReaders = new ArrayList<>();
        if (globalSortedSeriesDataSource.getUnsealedTsFile() != null) {
            for (TimeSeriesChunkMetaData tscMetaData : globalSortedSeriesDataSource.getUnsealedTsFile().getTimeSeriesChunkMetaDatas()) {
                if (tscMetaData.getVInTimeSeriesChunkMetaData() != null) {
                    TsRandomAccessLocalFileReader fileReader = FileReaderMap.getInstance().get(globalSortedSeriesDataSource.getUnsealedTsFile().getFilePath());

                    ValueReader valueReader = new ValueReader(tscMetaData.getProperties().getFileOffset(),
                            tscMetaData.getTotalByteSize(),
                            tscMetaData.getVInTimeSeriesChunkMetaData().getDataType(),
                            tscMetaData.getVInTimeSeriesChunkMetaData().getDigest(), fileReader,
                            tscMetaData.getVInTimeSeriesChunkMetaData().getEnumValues(),
                            tscMetaData.getProperties().getCompression(), tscMetaData.getNumRows(),
                            tscMetaData.getTInTimeSeriesChunkMetaData().getStartTime(), tscMetaData.getTInTimeSeriesChunkMetaData().getEndTime());
                    valueReaders.add(valueReader);
                }
            }
        }

        if (globalSortedSeriesDataSource.getRawSeriesChunk() != null)
            memRawSeriesChunk = globalSortedSeriesDataSource.getRawSeriesChunk();

        overflowSeriesInsertReader = SeriesReaderFactory.getInstance().createSeriesReaderForOverflowInsert(overflowSeriesDataSource);
        overflowOperationReader = overflowSeriesDataSource.getUpdateDeleteInfoOfOneSeries().getOverflowUpdateOperationReader();
        overflowOperationReaderCopy = overflowOperationReader.copy();

        this.dataType = MManager.getInstance().getSeriesType(deltaObjectId + "." + measurementId);
        insertMemoryData = new InsertDynamicData(dataType, queryTimeFilter, queryValueFilter, globalSortedSeriesDataSource.getRawSeriesChunk(),
                overflowSeriesInsertReader, overflowOperationReader);
        this.deltaObjectId = deltaObjectId;
        this.measurementId = measurementId;
        this.queryTimeFilter = queryTimeFilter;
        if (queryTimeFilter != null) {
            singleTimeVisitor = getSingleValueVisitorByDataType(TSDataType.INT64, queryTimeFilter);
        }
        this.queryValueFilter = queryValueFilter;
        if (queryValueFilter != null) {
            singleValueVisitor = getSingleValueVisitorByDataType(dataType, queryValueFilter);
        }
    }

    public void closeFileStream() {
        tsFileReaderManager.closeFileStream();
    }

    public void closeFileStreamForOneRequest() throws IOException {
        tsFileReaderManager.clearReaderMaps();
        overflowSeriesInsertReader.close();
    }

    public InsertDynamicData getInsertMemoryData() {
        return this.insertMemoryData;
    }
}
