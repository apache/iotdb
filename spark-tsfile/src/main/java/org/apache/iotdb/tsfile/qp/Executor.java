package org.apache.iotdb.tsfile.qp;

import org.apache.iotdb.tsfile.read.ReadOnlyTsFile;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * This class used to execute Queries on TSFile
 */
public class Executor {
    public static List<QueryDataSet> query(ReadOnlyTsFile reader, List<QueryExpression> queryExpressions, long start, long end) {
        List<QueryDataSet> dataSets = new ArrayList<>();
        try {
            for(QueryExpression expression: queryExpressions) {
                QueryDataSet queryDataSet = reader.query(expression, start, end);
                dataSets.add(queryDataSet);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return dataSets;
    }
}
