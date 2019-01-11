package cn.edu.tsinghua.tsfile.qp;

import cn.edu.tsinghua.tsfile.common.utils.ITsRandomAccessFileReader;
import cn.edu.tsinghua.tsfile.read.query.QueryConfig;
import cn.edu.tsinghua.tsfile.read.query.QueryDataSet;
import cn.edu.tsinghua.tsfile.read.query.QueryEngine;

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
