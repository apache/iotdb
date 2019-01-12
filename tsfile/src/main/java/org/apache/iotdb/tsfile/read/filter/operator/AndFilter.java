package org.apache.iotdb.tsfile.read.filter.operator;

import org.apache.iotdb.tsfile.read.filter.DigestForFilter;
import org.apache.iotdb.tsfile.read.filter.basic.BinaryFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

/**
 * Both the left and right operators of AndExpression must satisfy the condition.
 */
public class AndFilter extends BinaryFilter {

    private static final long serialVersionUID = 6705254093824897938L;

    public AndFilter(Filter left, Filter right) {
        super(left, right);
    }

    @Override
    public boolean satisfy(DigestForFilter digest) {
        return left.satisfy(digest) && right.satisfy(digest);
    }

    @Override
    public boolean satisfy(long time, Object value) {
        return left.satisfy(time, value) && right.satisfy(time, value);
    }

    @Override
    public boolean satisfyStartEndTime(long startTime, long endTime) {
        return left.satisfyStartEndTime(startTime, endTime) && right.satisfyStartEndTime(startTime, endTime);
    }

    @Override
    public String toString() {
        return "(" + left + " && " + right + ")";
    }
}
