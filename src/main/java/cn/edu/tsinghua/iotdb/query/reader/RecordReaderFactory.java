package cn.edu.tsinghua.iotdb.query.reader;

import cn.edu.tsinghua.iotdb.engine.filenode.FileNodeManager;
import cn.edu.tsinghua.iotdb.engine.querycontext.QueryDataSource;
import cn.edu.tsinghua.iotdb.exception.FileNodeManagerException;
import cn.edu.tsinghua.iotdb.exception.PathErrorException;
import cn.edu.tsinghua.iotdb.query.management.ReadCacheManager;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.impl.QueryFilterFactory;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.impl.SeriesFilter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.operator.NoRestriction;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * To avoid create RecordReader frequently,<br>
 * RecordReaderFactory could create a RecordReader using cache.
 *
 * @author Jinrui Zhang
 */
public class RecordReaderFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(RecordReaderFactory.class);
    private static RecordReaderFactory instance = new RecordReaderFactory();

    private FileNodeManager fileNodeManager;
    private ReadCacheManager readCacheManager;

    private RecordReaderFactory() {
        fileNodeManager = FileNodeManager.getInstance();
        readCacheManager = ReadCacheManager.getInstance();
    }

    /**
     * Construct a RecordReader which contains QueryStructure and read lock token.
     *
     * @param readLock if readLock is not null, the read lock of file node has been created,<br>
     *                 else a new read lock token should be applied.
     * @param prefix   for the exist of <code>RecordReaderCacheManager</code> and batch read, we need a prefix to
     *                 represent the uniqueness.
     * @return <code>RecordReader</code>
     */
    public synchronized RecordReader getRecordReader(String deltaObjectUID, String measurementID,
                                        SingleSeriesFilterExpression timeFilter, SingleSeriesFilterExpression valueFilter,
                                        Integer readLock, String prefix, ReaderType readerType)
            throws ProcessorException, PathErrorException, IOException {
        int readToken = 0;
        if (readLock == null) {
            readToken = readCacheManager.lock(deltaObjectUID);
        } else {
            readToken = readLock;
        }
        String cacheDeltaKey = prefix + deltaObjectUID;
        if (readCacheManager.getRecordReaderCacheManager().containsRecordReader(cacheDeltaKey, measurementID)) {
            return readCacheManager.getRecordReaderCacheManager().get(cacheDeltaKey, measurementID);
        } else {
            QueryDataSource queryDataSource;
            try {
                SeriesFilter seriesFilter = new SeriesFilter<>(new Path(deltaObjectUID+"."+measurementID), new NoRestriction());
                queryDataSource = fileNodeManager.query(seriesFilter);
            } catch (FileNodeManagerException e) {
                throw new ProcessorException(e.getMessage());
            }

            RecordReader recordReader = createANewRecordReader(deltaObjectUID, measurementID, timeFilter, valueFilter, queryDataSource, readerType, readToken);
            readCacheManager.getRecordReaderCacheManager().put(cacheDeltaKey, measurementID, recordReader);
            return recordReader;
        }
    }

    private RecordReader createANewRecordReader(String deltaObjectUID, String measurementID,
                                                SingleSeriesFilterExpression queryTimeFilter, SingleSeriesFilterExpression queryValueFilter,
                                                QueryDataSource queryDataSource, ReaderType readerType, int readToken) throws PathErrorException, IOException {
        switch (readerType) {
            case QUERY:
                return new QueryRecordReader(queryDataSource.getSeriesDataSource(), queryDataSource.getOverflowSeriesDataSource(),
                        deltaObjectUID, measurementID, queryTimeFilter, queryValueFilter, readToken);
            case AGGREGATE:
                return new AggregateRecordReader(queryDataSource.getSeriesDataSource(), queryDataSource.getOverflowSeriesDataSource(),
                        deltaObjectUID, measurementID, queryTimeFilter, queryValueFilter, readToken);
            case FILL:
                return new FillRecordReader(queryDataSource.getSeriesDataSource(), queryDataSource.getOverflowSeriesDataSource(),
                        deltaObjectUID, measurementID, queryTimeFilter, queryValueFilter, readToken);
            case GROUPBY:
                return new QueryRecordReader(queryDataSource.getSeriesDataSource(), queryDataSource.getOverflowSeriesDataSource(),
                        deltaObjectUID, measurementID, queryTimeFilter, queryValueFilter, readToken);
        }

        return null;
    }

    public static RecordReaderFactory getInstance() {
        return instance;
    }

    // TODO this method is only used in test case and KV-match index
    public void removeRecordReader(String deltaObjectId, String measurementId) throws IOException {
        if (readCacheManager.getRecordReaderCacheManager().containsRecordReader(deltaObjectId, measurementId)) {
            // close the RecordReader read stream.
            readCacheManager.getRecordReaderCacheManager().get(deltaObjectId, measurementId).closeFileStream();
            readCacheManager.getRecordReaderCacheManager().get(deltaObjectId, measurementId).closeFileStreamForOneRequest();
            readCacheManager.getRecordReaderCacheManager().remove(deltaObjectId, measurementId);
        }
    }
}
