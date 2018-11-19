package cn.edu.tsinghua.tsfile.timeseries.filter.utils;

/**
 * used for filter optimization for example, FilterExpression And(And(GtEq(10), LtEq(20)),
 * And(GtEq(30), LtEq(40))), when we use SingleValueVistor, to examine whether a value satisfy the
 * FilterExpression, we need invoke the filter comparison methods (generic transform) at most 7
 * times, but the basic type comparison(int, long, float, double) will cost less time than generic
 * transform, so we can transform Or(And(GtEq(10), LtEq(20)), And(GtEq(30), LtEq(40))) to an array,
 * v[0]=10, v[1]=20, v[2]=30, v[3]=40.
 *
 * @author CGF
 */
public abstract class Interval {
    // value array max num
    protected static final int arrayMaxn = 100;
    // visit array to judge whether pos[i] and pos[i+1] could be reached
    // flag[i]=true represents that v[i] could be reached
    // flag[i]=false represents that v[i] could not be reached
    public boolean[] flag = new boolean[arrayMaxn];
    // to identify the last position of array
    public int count = 0;

    public Interval() {
        this.count = 0;
    }
}
