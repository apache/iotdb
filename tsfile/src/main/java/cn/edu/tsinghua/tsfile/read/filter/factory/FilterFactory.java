package cn.edu.tsinghua.tsfile.read.filter.factory;

import cn.edu.tsinghua.tsfile.read.filter.basic.Filter;
import cn.edu.tsinghua.tsfile.read.filter.operator.AndFilter;
import cn.edu.tsinghua.tsfile.read.filter.operator.NotFilter;
import cn.edu.tsinghua.tsfile.read.filter.operator.OrFilter;

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
