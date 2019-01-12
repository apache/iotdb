package org.apache.iotdb.tsfile.constant;

import java.util.Random;

/**
 * This class is used for Junit test to get some unified constant.
 * 
 * @author kangrong
 *
 */
public class TimeseriesTestConstant {
    public static final float float_min_delta = 0.00001f;
    public static final double double_min_delta = 0.00001d;
    public static final Random random = new Random(System.currentTimeMillis());
}
