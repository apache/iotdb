package cn.edu.tsinghua.tsfile.timeseries.filter.verifier;

import cn.edu.tsinghua.tsfile.timeseries.filter.definition.FilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.operators.*;
import cn.edu.tsinghua.tsfile.timeseries.filter.utils.DoubleInterval;
import cn.edu.tsinghua.tsfile.timeseries.filter.utils.Interval;
import cn.edu.tsinghua.tsfile.timeseries.filter.visitorImpl.ConvertExpressionVisitor;
import cn.edu.tsinghua.tsfile.timeseries.filter.visitorImpl.FilterVisitor;

/**
 * @author CGF
 */
public class DoubleFilterVerifier extends FilterVerifier implements FilterVisitor<DoubleInterval> {
    private Double DOUBLE_MIN_VALUE = -Double.MAX_VALUE;

    private ConvertExpressionVisitor convertor = new ConvertExpressionVisitor();

    @Override
    public Interval getInterval(SingleSeriesFilterExpression filter) {
        if (filter == null) {
            DoubleInterval ans = new DoubleInterval();
            ans.addValueFlag(DOUBLE_MIN_VALUE, true);
            ans.addValueFlag(Double.MAX_VALUE, true);
            return ans;
        }

        return filter.accept(this);
    }

    @Override
    public <T extends Comparable<T>> DoubleInterval visit(Eq<T> eq) {
        DoubleInterval ans = new DoubleInterval();
        ans.v[0] = ((Double) eq.getValue()).doubleValue();
        ans.v[1] = ((Double) eq.getValue()).doubleValue();
        ans.flag[0] = true;
        ans.flag[1] = true;
        ans.count = 2;
        return ans;
    }

    @Override
    public <T extends Comparable<T>> DoubleInterval visit(NotEq<T> notEq) {
        DoubleInterval ans = new DoubleInterval();
        ans.v[0] = DOUBLE_MIN_VALUE;
        ans.v[1] = ((Double) notEq.getValue()).doubleValue();
        ans.v[2] = ((Double) notEq.getValue()).doubleValue();
        ans.v[3] = Double.MAX_VALUE;

        if ((Double) notEq.getValue() == DOUBLE_MIN_VALUE) {
            ans.flag[0] = false;
            ans.flag[1] = false;
            ans.flag[2] = false;
            ans.flag[3] = true;
        } else if ((Double) notEq.getValue() == Double.MAX_VALUE) {
            ans.flag[0] = true;
            ans.flag[1] = false;
            ans.flag[2] = false;
            ans.flag[3] = false;
        } else {
            ans.flag[0] = true;
            ans.flag[1] = false;
            ans.flag[2] = false;
            ans.flag[3] = true;
        }

        ans.count = 4;
        return ans;
    }

    @Override
    public <T extends Comparable<T>> DoubleInterval visit(LtEq<T> ltEq) {
        DoubleInterval ans = new DoubleInterval();
        if (ltEq.ifEq) {
            ans.v[1] = ((Double) ltEq.getValue()).doubleValue();
            ans.flag[1] = true;
        } else {
            ans.v[1] = ((Double) ltEq.getValue()).doubleValue();
            ans.flag[1] = false;
        }

        if (ans.v[1] == DOUBLE_MIN_VALUE && !ans.flag[1])
            ans.flag[0] = false;
        else
            ans.flag[0] = true;
        ans.v[0] = DOUBLE_MIN_VALUE;

        ans.count = 2;
        return ans;
    }

    @Override
    public <T extends Comparable<T>> DoubleInterval visit(GtEq<T> gtEq) {
        DoubleInterval ans = new DoubleInterval();
        if (gtEq.ifEq) {
            ans.v[0] = ((Double) gtEq.getValue()).doubleValue();
            ans.flag[0] = true;
        } else {
            ans.v[0] = ((Double) gtEq.getValue()).doubleValue();
            ans.flag[0] = false;
        }

        ans.v[1] = Double.MAX_VALUE;
        if (ans.v[0] == Double.MAX_VALUE && !ans.flag[0])
            ans.flag[1] = false;
        else
            ans.flag[1] = true;

        ans.count = 2;
        return ans;
    }

    @Override
    public DoubleInterval visit(Not not) {
        return visit(convertor.convert(not));
    }

    public DoubleInterval visit(FilterExpression filter) {
        if (filter instanceof Eq)
            return visit((Eq<?>) filter);
        else if (filter instanceof NotEq)
            return visit((NotEq<?>) filter);
        else if (filter instanceof LtEq)
            return visit((LtEq<?>) filter);
        else if (filter instanceof GtEq)
            return visit((GtEq<?>) filter);
        else if (filter instanceof And)
            return visit((And) filter);
        else if (filter instanceof Or)
            return visit((Or) filter);
        return null;
    }

    @Override
    public DoubleInterval visit(And and) {
        return intersection(visit(and.getLeft()), visit(and.getRight()));
    }

    @Override
    public DoubleInterval visit(Or or) {
        return union(visit(or.getLeft()), visit(or.getRight()));
    }

    @Override
    public DoubleInterval visit(NoFilter noFilter) {
        DoubleInterval ans = new DoubleInterval();
        ans.v[0] = DOUBLE_MIN_VALUE;
        ans.flag[0] = true;
        ans.v[1] = Double.MAX_VALUE;
        ans.flag[1] = true;
        return ans;
    }

    private DoubleInterval intersection(DoubleInterval left, DoubleInterval right) {
        DoubleInterval ans = new DoubleInterval();
        DoubleInterval partResult = new DoubleInterval();

        for (int i = 0; i < left.count; i += 2) {
            for (int j = 0; j < right.count; j += 2) {
                if (left.v[i + 1] <= right.v[j]) {
                    if (left.v[i + 1] == right.v[j] && left.flag[i + 1]
                            && right.flag[j]) {
                        partResult.addValueFlag(left.v[i + 1], true);
                        partResult.addValueFlag(left.v[i + 1], true);
                    } else {
                        break;
                    }
                } else if (left.v[i] >= right.v[j + 1]) {
                    if (left.v[i] == right.v[j + 1] && (left.flag[i] && right.flag[j + 1])) {
                        partResult.addValueFlag(left.v[i], true);
                        partResult.addValueFlag(left.v[i], true);
                    }
                } else {
                    if (left.v[i] > right.v[j]) {
                        partResult.addValueFlag(left.v[i], left.flag[i]);
                    } else {
                        partResult.addValueFlag(right.v[j], right.flag[j]);
                    }
                    if (left.v[i + 1] > right.v[j + 1]) {
                        partResult.addValueFlag(right.v[j + 1], right.flag[j + 1]);
                    } else {
                        partResult.addValueFlag(left.v[i + 1], left.flag[i + 1]);
                    }
                }
            }

            for (int cnt = 0; cnt < partResult.count; cnt++) {
                ans.addValueFlag(partResult.v[cnt], partResult.flag[cnt]);
            }
            partResult.count = 0;
        }

        return ans;
    }

    private DoubleInterval union(DoubleInterval left, DoubleInterval right) {
        int l = 0, r = 0;
        DoubleInterval res = new DoubleInterval();
        while (l < left.count || r < right.count) {
            if (l >= left.count) { // only right has unmerged data, all right data should be added to ans
                for (int i = r; i < right.count; i += 2) {
                    res.addValueFlag(right.v[i], right.flag[i]);
                    res.addValueFlag(right.v[i + 1], right.flag[i + 1]);
                }
                break;
            }
            if (r >= right.count) { // only left has unmerged data, all left data should be added to ans
                for (int i = l; i < left.count; i += 2) {
                    res.addValueFlag(left.v[i], left.flag[i]);
                    res.addValueFlag(left.v[i + 1], left.flag[i + 1]);
                }
                break;
            }

            if (left.v[l] >= right.v[r + 1]) { // right first
                res.addValueFlag(right.v[r], right.flag[r]);
                res.addValueFlag(right.v[r + 1], right.flag[r + 1]);
                r += 2;
            } else if (left.v[l] >= right.v[r] && left.v[l] <= right.v[r + 1] && left.v[l + 1] >= right.v[r + 1]) { // right first cross
                if (left.v[l] == right.v[r]) {
                    res.addValueFlag(left.v[l], left.flag[l] | right.flag[r]);
                } else {
                    res.addValueFlag(right.v[r], right.flag[r]);
                }
                if (left.v[l + 1] == right.v[r + 1]) {
                    res.addValueFlag(left.v[l + 1], left.flag[l + 1] | right.flag[r + 1]);
                    l += 2;
                    r += 2;
                } else {
                    res.addValueFlag(right.v[r + 1], right.flag[r + 1]);
                    left.v[l] = right.v[r + 1];
                    left.flag[l] = !right.flag[r + 1];
                    r += 2;
                }
            } else if (left.v[l] <= right.v[r] && left.v[l + 1] >= right.v[r + 1]) { // left covers right
                res.addValueFlag(left.v[l], left.flag[l]);
                if (left.v[l + 1] == right.v[r + 1]) {
                    res.addValueFlag(left.v[l + 1], left.flag[l + 1] | right.flag[r + 1]);
                    l += 2;
                    r += 2;
                } else {
                    res.addValueFlag(right.v[r + 1], right.flag[r + 1]);
                    left.v[l] = right.v[r + 1];
                    left.flag[l] = !right.flag[r + 1];
                    r += 2;
                }
            } else if (right.v[r] >= left.v[l] && right.v[r] <= left.v[l + 1] && left.v[l + 1] <= right.v[r + 1]) { // left first cross
                if (left.v[l] == right.v[r]) {
                    res.addValueFlag(left.v[l], left.flag[l] | right.flag[r]);
                } else {
                    res.addValueFlag(left.v[l], left.flag[l]);
                }
                // left covers right contains (left.v[l+1]==right.v[r+1])
                res.addValueFlag(left.v[l + 1], left.flag[l + 1]);
                if (left.v[l + 1] == right.v[r]) {
                    right.v[r] = left.v[l + 1];
                    right.flag[r] = left.flag[l + 1] | right.flag[r];
                    l += 2;
                } else {
                    right.v[r] = left.v[l + 1];
                    right.flag[r] = !left.flag[l + 1];
                    l += 2;
                }
            } else if (left.v[l + 1] <= right.v[r]) { // left first
                res.addValueFlag(left.v[l], left.flag[l]);
                res.addValueFlag(left.v[l + 1], left.flag[l + 1]);
                l += 2;
            } else { // right covers left
                res.addValueFlag(right.v[r], right.flag[r]);
                // right first cross contains (left.v[l+1] == right.v[r+1])
                res.addValueFlag(left.v[l + 1], left.flag[l + 1]);
                right.v[r] = left.v[l + 1];
                right.flag[r] = !left.flag[l + 1];
                l += 2;
            }
        }
        // merge same value into one
        DoubleInterval ans = new DoubleInterval();
        if (res.count == 0)
            return res;
        ans.addValueFlag(res.v[0], res.flag[0]);
        ans.addValueFlag(res.v[1], res.flag[1]);
        for (int i = 2; i < res.count; i += 2) {
            if (res.v[i] == ans.v[ans.count - 1] && (res.flag[i] || ans.flag[ans.count - 1])) {
                if (res.v[i + 1] == ans.v[ans.count - 1]) {
                    ans.flag[ans.count - 1] = ans.flag[ans.count - 1] | res.flag[i + 1];
                } else {
                    ans.v[ans.count - 1] = res.v[i + 1];
                    ans.flag[ans.count - 1] = res.flag[i + 1];
                }
            } else {
                ans.addValueFlag(res.v[i], res.flag[i]);
                ans.addValueFlag(res.v[i + 1], res.flag[i + 1]);
            }
        }
        return ans;
    }
}


