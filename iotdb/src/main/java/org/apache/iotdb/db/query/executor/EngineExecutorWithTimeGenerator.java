package org.apache.iotdb.db.query.executor;

import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.exception.FileNodeManagerException;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.query.control.QueryDataSourceManager;
import org.apache.iotdb.db.query.control.QueryTokenManager;
import org.apache.iotdb.db.query.dataset.EngineDataSetWithTimeGenerator;
import org.apache.iotdb.db.query.factory.SeriesReaderFactory;
import org.apache.iotdb.db.query.reader.merge.EngineReaderByTimeStamp;
import org.apache.iotdb.db.query.reader.merge.PriorityMergeReader;
import org.apache.iotdb.db.query.reader.merge.PriorityMergeReaderByTimestamp;
import org.apache.iotdb.db.query.reader.sequence.SequenceDataReader;
import org.apache.iotdb.db.query.timegenerator.EngineTimeGenerator;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.exception.FileNodeManagerException;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.query.control.QueryDataSourceManager;
import org.apache.iotdb.db.query.control.QueryTokenManager;
import org.apache.iotdb.db.query.dataset.EngineDataSetWithTimeGenerator;
import org.apache.iotdb.db.query.factory.SeriesReaderFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * IoTDB query executor with filter
 */
public class EngineExecutorWithTimeGenerator {

    private QueryExpression queryExpression;
    private long jobId;

    EngineExecutorWithTimeGenerator(long jobId, QueryExpression queryExpression) {
        this.jobId = jobId;
        this.queryExpression = queryExpression;
    }

    public QueryDataSet execute() throws IOException, FileNodeManagerException {

        QueryTokenManager.getInstance().beginQueryOfGivenQueryPaths(jobId, queryExpression.getSelectedSeries());
        QueryTokenManager.getInstance().beginQueryOfGivenExpression(jobId, queryExpression.getExpression());

        EngineTimeGenerator timestampGenerator = new EngineTimeGenerator(jobId, queryExpression.getExpression());

        List<EngineReaderByTimeStamp> readersOfSelectedSeries = getReadersOfSelectedPaths(queryExpression.getSelectedSeries());

        List<TSDataType> dataTypes = new ArrayList<>();

        for (Path path : queryExpression.getSelectedSeries()) {
            try {
                dataTypes.add(MManager.getInstance().getSeriesType(path.getFullPath()));
            } catch (PathErrorException e) {
                throw new FileNodeManagerException(e);
            }

        }
        return new EngineDataSetWithTimeGenerator(queryExpression.getSelectedSeries(), dataTypes,
                timestampGenerator, readersOfSelectedSeries);
    }

    private List<EngineReaderByTimeStamp> getReadersOfSelectedPaths(List<Path> paths)
            throws IOException, FileNodeManagerException {

        List<EngineReaderByTimeStamp> readersOfSelectedSeries = new ArrayList<>();

        for (Path path : paths) {

            QueryDataSource queryDataSource = QueryDataSourceManager.getQueryDataSource(jobId, path);

            PriorityMergeReaderByTimestamp mergeReaderByTimestamp = new PriorityMergeReaderByTimestamp();

            // reader for sequence data
            SequenceDataReader tsFilesReader = new SequenceDataReader(queryDataSource.getSeqDataSource(), null);
            mergeReaderByTimestamp.addReaderWithPriority(tsFilesReader, 1);

            // reader for unSequence data
            PriorityMergeReader unSeqMergeReader = SeriesReaderFactory.getInstance().
                    createUnSeqMergeReader(queryDataSource.getOverflowSeriesDataSource(), null);
            mergeReaderByTimestamp.addReaderWithPriority(unSeqMergeReader, 2);

            readersOfSelectedSeries.add(mergeReaderByTimestamp);
        }

        return readersOfSelectedSeries;
    }

}
