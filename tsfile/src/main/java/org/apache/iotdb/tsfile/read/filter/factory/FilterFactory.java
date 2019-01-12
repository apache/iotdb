package org.apache.iotdb.tsfile.read.filter.factory;

import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.operator.AndFilter;
import org.apache.iotdb.tsfile.read.filter.operator.NotFilter;
import org.apache.iotdb.tsfile.read.filter.operator.OrFilter;
import org.apache.iotdb.tsfile.read.filter.operator.AndFilter;
import org.apache.iotdb.tsfile.read.filter.operator.NotFilter;

public class FilterFactory {
    public static AndFilter and(Filter left, Filter right){
        return new AndFilter(left, right);
    }

    public static OrFilter or(Filter left, Filter right){
        return new OrFilter(left, right);
    }

    public static NotFilter not(Filter filter) {
        return new NotFilter(filter);
    }

}
