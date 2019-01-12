package org.apache.iotdb.tsfile.qp;

import org.apache.iotdb.tsfile.common.utils.ITsRandomAccessFileReader;
import org.apache.iotdb.tsfile.read.query.QueryConfig;
import org.apache.iotdb.tsfile.read.query.QueryDataSet;
import org.apache.iotdb.tsfile.read.query.QueryEngine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * This class used to execute Queries on TSFile
 */
public class Executor {
    public static List<QueryDataSet> query(ITsRandomAccessFileReader in, List<QueryConfig> queryConfigs, HashMap<String, Long> parameters) {
        QueryEngine queryEngine;
        List<QueryDataSet> dataSets = new ArrayList<>();
        try {
            queryEngine = new QueryEngine(in);
            for(QueryConfig queryConfig: queryConfigs) {
                QueryDataSet queryDataSet = queryEngine.query(queryConfig, parameters);
                dataSets.add(queryDataSet);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return dataSets;
    }
}
