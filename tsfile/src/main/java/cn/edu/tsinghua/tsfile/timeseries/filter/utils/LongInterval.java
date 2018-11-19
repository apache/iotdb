package cn.edu.tsinghua.tsfile.timeseries.filter.utils;

import cn.edu.tsinghua.tsfile.common.exception.FilterInvokeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * used for filter LONG type optimization see {@link Interval}
 *
 * @author CGF
 */
public class LongInterval extends Interval {
    private static final Logger LOG = LoggerFactory.getLogger(LongInterval.class);

    // long value array
    public long[] v = new long[arrayMaxn];

    public void addValueFlag(long value, boolean f) {
        if (count >= arrayMaxn - 2) {
            LOG.error("IntInterval array length spill.");
            throw new FilterInvokeException("LongInterval array length spill.");
        }
        v[count] = value;
        flag[count] = f;
        count++;
    }

    public String toString() {
        StringBuffer ans = new StringBuffer();
        for (int i = 0; i < count; i += 2) {
            if (flag[i])
                ans.append("[" + v[i] + ",");
            else
                ans.append("(" + v[i] + ",");
            if (flag[i + 1])
                ans.append(v[i + 1] + "]");
            else
                ans.append(v[i + 1] + ")");
        }
        return ans.toString();
    }
}
