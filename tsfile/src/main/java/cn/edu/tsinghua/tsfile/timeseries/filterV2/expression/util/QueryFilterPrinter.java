package cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.util;

import cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.BinaryQueryFilter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.QueryFilter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.UnaryQueryFilter;

/**
 * Created by zhangjinrui on 2017/12/19.
 */
public class QueryFilterPrinter {

    private static final int MAX_DEPTH = 100;
    private static final char PREFIX_CHAR = '\t';
    private static final String[] PREFIX = new String[MAX_DEPTH];

    static {
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < MAX_DEPTH; i++) {
            PREFIX[i] = stringBuilder.toString();
            stringBuilder.append(PREFIX_CHAR);
        }
    }

    public static void print(QueryFilter queryFilter) {
        print(queryFilter, 0);
    }

    private static void print(QueryFilter queryFilter, int level) {
        if (queryFilter instanceof UnaryQueryFilter) {
            System.out.println(getPrefix(level) + queryFilter);
        } else {
            System.out.println(getPrefix(level) + queryFilter.getType() + ":");
            print(((BinaryQueryFilter)queryFilter).getLeft(), level + 1);
            print(((BinaryQueryFilter)queryFilter).getRight(), level + 1);
        }
    }

    private static String getPrefix(int count) {
        if (count < MAX_DEPTH) {
            return PREFIX[count];
        } else {
            return PREFIX[MAX_DEPTH - 1];
        }
    }
}
