package cn.edu.tsinghua.iotdb.query.control;

import cn.edu.tsinghua.iotdb.engine.filenode.FileNodeManager;
import cn.edu.tsinghua.iotdb.engine.querycontext.QueryDataSource;
import cn.edu.tsinghua.iotdb.exception.FileNodeManagerException;
import cn.edu.tsinghua.tsfile.read.common.Path;
import cn.edu.tsinghua.tsfile.read.expression.impl.SingleSeriesExpression;

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
