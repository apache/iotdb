package cn.edu.tsinghua.tsfile.read.filter.basic;


import cn.edu.tsinghua.tsfile.read.filter.DigestForFilter;

/**
 * Filter is a top level filter abstraction.
 *
 */
public interface Filter {

    /**
     * To examine whether the digest is satisfied with the filter.
     *
     * @param digest digest with min time, max time, min value, max value.
     */
    boolean satisfy(DigestForFilter digest);

    /**
     * To examine whether the single point(with time and value) is satisfied with the filter.
     *
     * @param time single point time
     * @param value single point value
     */
    boolean satisfy(long time, Object value);

    /**
     * To examine whether the min time and max time are satisfied with the filter.
     *
     * @param startTime start time of a page, series or device
     * @param endTime end time of a page, series or device
     */
    boolean satisfyStartEndTime(long startTime, long endTime);
}
