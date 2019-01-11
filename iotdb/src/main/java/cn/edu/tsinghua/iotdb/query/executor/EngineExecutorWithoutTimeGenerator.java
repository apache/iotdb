package cn.edu.tsinghua.iotdb.query.executor;

import cn.edu.tsinghua.iotdb.engine.querycontext.QueryDataSource;
import cn.edu.tsinghua.iotdb.exception.FileNodeManagerException;
import cn.edu.tsinghua.iotdb.exception.PathErrorException;
import cn.edu.tsinghua.iotdb.metadata.MManager;
import cn.edu.tsinghua.iotdb.query.control.QueryDataSourceManager;
import cn.edu.tsinghua.iotdb.query.control.QueryTokenManager;
import cn.edu.tsinghua.iotdb.query.dataset.EngineDataSetWithoutTimeGenerator;
import cn.edu.tsinghua.iotdb.query.factory.SeriesReaderFactory;
import cn.edu.tsinghua.iotdb.query.reader.IReader;
import cn.edu.tsinghua.iotdb.query.reader.merge.PriorityMergeReader;
import cn.edu.tsinghua.iotdb.query.reader.sequence.SequenceDataReader;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.read.common.Path;
import cn.edu.tsinghua.tsfile.read.expression.QueryExpression;
import cn.edu.tsinghua.tsfile.read.expression.impl.GlobalTimeExpression;
import cn.edu.tsinghua.tsfile.read.filter.basic.Filter;
import cn.edu.tsinghua.tsfile.read.query.dataset.QueryDataSet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * IoTDB query executor with  global time filter
 */
public class EngineExecutorWithoutTimeGenerator {

    private QueryExpression queryExpression;
    private long jobId;

    public EngineExecutorWithoutTimeGenerator(long jobId, QueryExpression queryExpression) {
        this.jobId = jobId;
        this.queryExpression = queryExpression;
    }

    /**
     * with global time filter
     */
    public QueryDataSet executeWithGlobalTimeFilter()
            throws IOException, FileNodeManagerException, PathErrorException {

        Filter timeFilter = ((GlobalTimeExpression) queryExpression.getExpression()).getFilter();

        List<IReader> readersOfSelectedSeries = new ArrayList<>();
        List<TSDataType> dataTypes = new ArrayList<>();

        QueryTokenManager.getInstance().beginQueryOfGivenQueryPaths(jobId, queryExpression.getSelectedSeries());

        for (Path path : queryExpression.getSelectedSeries()) {

            QueryDataSource queryDataSource = QueryDataSourceManager.getQueryDataSource(jobId, path);

            // add data type
            dataTypes.add(MManager.getInstance().getSeriesType(path.getFullPath()));

            PriorityMergeReader priorityReader = new PriorityMergeReader();

            // sequence reader for one sealed tsfile
            SequenceDataReader tsFilesReader = new SequenceDataReader(queryDataSource.getSeqDataSource(), timeFilter);
            priorityReader.addReaderWithPriority(tsFilesReader, 1);

            // unseq reader for all chunk groups in unSeqFile
            PriorityMergeReader unSeqMergeReader = SeriesReaderFactory.getInstance().
                    createUnSeqMergeReader(queryDataSource.getOverflowSeriesDataSource(), timeFilter);
            priorityReader.addReaderWithPriority(unSeqMergeReader, 2);

            readersOfSelectedSeries.add(priorityReader);
        }

        return new EngineDataSetWithoutTimeGenerator(queryExpression.getSelectedSeries(), dataTypes, readersOfSelectedSeries);

    }

    /**
     * without filter
     */
    public QueryDataSet executeWithoutFilter()
            throws IOException, FileNodeManagerException, PathErrorException {

        List<IReader> readersOfSelectedSeries = new ArrayList<>();
        List<TSDataType> dataTypes = new ArrayList<>();

        QueryTokenManager.getInstance().beginQueryOfGivenQueryPaths(jobId, queryExpression.getSelectedSeries());

        for (Path path : queryExpression.getSelectedSeries()) {

            QueryDataSource queryDataSource = QueryDataSourceManager.getQueryDataSource(jobId, path);

            // add data type
            dataTypes.add(MManager.getInstance().getSeriesType(path.getFullPath()));

            PriorityMergeReader priorityReader = new PriorityMergeReader();

            // sequence insert data
            SequenceDataReader tsFilesReader = new SequenceDataReader(queryDataSource.getSeqDataSource(), null);
            priorityReader.addReaderWithPriority(tsFilesReader, 1);

            // unseq insert data
            PriorityMergeReader unSeqMergeReader = SeriesReaderFactory.getInstance().
                    createUnSeqMergeReader(queryDataSource.getOverflowSeriesDataSource(), null);
            priorityReader.addReaderWithPriority(unSeqMergeReader, 2);

            readersOfSelectedSeries.add(priorityReader);
        }

        return new EngineDataSetWithoutTimeGenerator(queryExpression.getSelectedSeries(), dataTypes, readersOfSelectedSeries);
    }

}
