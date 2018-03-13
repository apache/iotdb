package cn.edu.tsinghua.iotdb.query.management;

/**
 * <p>
 * The methods of this class is used for batch read,
 * to avoid the stream conflict of select series and filter series.
 * </p>
 * <p>
 * <p>
 * For example, "select s1,s2 from table where time<10 or (s1>20 and s3<40)" will be
 * resolving to "select s1,s2 from table where time<10 and s1>20", "select s1,s2 from table where time<10 and s3<40",
 * in "select s1,s2 from table where time<10 and s1>20" process, calculating the time of "s1>20" will cause stream
 * offset in <code>RecordReaderFactory</code> and select clause may be wrong, so we provide a prefix parameter to
 * <code>RecordReaderFactory.getRecordReader()</code>
 * </p>
 *
 * @author CGF.
 */
public class ReadCachePrefix {

    public static String addQueryPrefix(int forNumber) {
        return String.format("QUERY.%s", forNumber);
    }

    public static String addQueryPrefix(String prefix, int forNumber) {
        return String.format("%s.QUERY.%s", prefix, forNumber);
    }

    public static String addFilterPrefix(int valueFilterNumber) {
        return String.format("FILTER.%s", valueFilterNumber);
    }

    public static String addFilterPrefix(String prefix, int valueFilterNumber) {
        return String.format("%s.FILTER.%s", prefix, valueFilterNumber);
    }
}
