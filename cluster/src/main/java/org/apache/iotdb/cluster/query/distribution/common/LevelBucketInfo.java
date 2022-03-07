package org.apache.iotdb.cluster.query.distribution.common;

import java.util.List;
import java.util.Map;

/**
 * This class is used to store all the buckets for the GroupByLevelOperator
 * It stores the levels index and all the enumerated values in each level by a HashMap
 * Using the HashMap, the operator could calculate all the buckets using combination of values from each level
 */
public class LevelBucketInfo {
    // eg: If the clause is `group by level = 1, 2, 3`, the map should be like
    // map{1 -> ['a', 'b'], 2 -> ['aa', 'bb'], 3 -> ['aaa', 'bbb']}
    private Map<Integer, List<String>> levelMap;
}
