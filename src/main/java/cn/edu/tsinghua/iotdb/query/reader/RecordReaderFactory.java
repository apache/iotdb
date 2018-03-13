package cn.edu.tsinghua.iotdb.query.reader;

import cn.edu.tsinghua.iotdb.engine.filenode.FileNodeManager;
import cn.edu.tsinghua.iotdb.engine.querycontext.QueryDataSource;
import cn.edu.tsinghua.iotdb.exception.FileNodeManagerException;
import cn.edu.tsinghua.iotdb.exception.PathErrorException;
import cn.edu.tsinghua.iotdb.query.management.ReadLockManager;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
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
    private ReadLockManager readLockManager;

    private RecordReaderFactory() {
        fileNodeManager = FileNodeManager.getInstance();
        readLockManager = ReadLockManager.getInstance();
    }

    /**
     * Construct a RecordReader which contains QueryStructure and read lock token.
     *
     * @param readLock if readLock is not null, the read lock of file node has been created,<br>
     *                 else a new read lock token should be applied.
     * @param prefix   for the exist of <code>RecordReaderCache</code> and batch read, we need a prefix to
     *                 represent the uniqueness.
     * @return <code>RecordReader</code>
     */
    public RecordReader getRecordReader(String deltaObjectUID, String measurementID,
                                        SingleSeriesFilterExpression timeFilter, SingleSeriesFilterExpression valueFilter,
                                        Integer readLock, String prefix, ReaderType readerType)
            throws ProcessorException, PathErrorException, IOException {
        int token = 0;
        if (readLock == null) {
            token = readLockManager.lock(deltaObjectUID);
        } else {
            token = readLock;
        }
        String cacheDeltaKey = prefix + deltaObjectUID;
        if (readLockManager.recordReaderCache.containsRecordReader(cacheDeltaKey, measurementID)) {
            return readLockManager.recordReaderCache.get(cacheDeltaKey, measurementID);
        } else {
            QueryDataSource queryDataSource;
            try {
                queryDataSource = fileNodeManager.query(deltaObjectUID, measurementID, timeFilter, null, valueFilter);
            } catch (FileNodeManagerException e) {
                throw new ProcessorException(e.getMessage());
            }
            RecordReader recordReader = createANewRecordReader(deltaObjectUID, measurementID, timeFilter, valueFilter, queryDataSource, readerType);
            readLockManager.recordReaderCache.put(cacheDeltaKey, measurementID, recordReader);
            return recordReader;
        }
    }

    private RecordReader createANewRecordReader(String deltaObjectUID, String measurementID,
                                                SingleSeriesFilterExpression queryTimeFilter, SingleSeriesFilterExpression queryValueFilter,
                                                QueryDataSource queryDataSource, ReaderType readerType) throws PathErrorException, IOException {
        switch (readerType) {
            case QUERY:
                return new QueryRecordReader(queryDataSource.getSeriesDataSource(), queryDataSource.getOverflowSeriesDataSource(),
                        deltaObjectUID, measurementID, queryTimeFilter, queryValueFilter);
            case AGGREGATE:
                return new AggregateRecordReader(queryDataSource.getSeriesDataSource(), queryDataSource.getOverflowSeriesDataSource(),
                        deltaObjectUID, measurementID, queryTimeFilter, queryValueFilter);
            case FILL:
                return new FillRecordReader(queryDataSource.getSeriesDataSource(), queryDataSource.getOverflowSeriesDataSource(),
                        deltaObjectUID, measurementID, queryTimeFilter, queryValueFilter);
            case GROUPBY:
                return new QueryRecordReader(queryDataSource.getSeriesDataSource(), queryDataSource.getOverflowSeriesDataSource(),
                        deltaObjectUID, measurementID, queryTimeFilter, queryValueFilter);
        }

        return null;
    }

    public static RecordReaderFactory getInstance() {
        return instance;
    }

    // TODO this method is only used in test case and KV-match index
    public void removeRecordReader(String deltaObjectId, String measurementId) throws IOException {
        if (readLockManager.recordReaderCache.containsRecordReader(deltaObjectId, measurementId)) {
            // close the RecordReader read stream.
            readLockManager.recordReaderCache.get(deltaObjectId, measurementId).closeFileStream();
            readLockManager.recordReaderCache.get(deltaObjectId, measurementId).closeFileStreamForOneRequest();
            readLockManager.recordReaderCache.remove(deltaObjectId, measurementId);
        }
    }
}
