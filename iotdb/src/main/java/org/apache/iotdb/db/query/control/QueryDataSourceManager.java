package org.apache.iotdb.db.query.control;

import org.apache.iotdb.db.engine.filenode.FileNodeManager;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.exception.FileNodeManagerException;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.db.engine.filenode.FileNodeManager;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.exception.FileNodeManagerException;

/**
 * <p> This class is used to get query data source of a given path.
 * See the component of <code>QueryDataSource</code>
 */
public class QueryDataSourceManager {

    private static FileNodeManager fileNodeManager = FileNodeManager.getInstance();

    public static QueryDataSource getQueryDataSource(long jobId, Path selectedPath) throws FileNodeManagerException {

        SingleSeriesExpression singleSeriesExpression = new SingleSeriesExpression(selectedPath, null);
        QueryDataSource queryDataSource = fileNodeManager.query(singleSeriesExpression);

        // add used files to current thread request cached map
        OpenedFilePathsManager.getInstance().addUsedFilesForCurrentRequestThread(jobId, queryDataSource);

        return queryDataSource;
    }
}
