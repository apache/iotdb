package cn.edu.tsinghua.iotdb.query;


import cn.edu.tsinghua.iotdb.query.management.ReadCachePrefix;
import org.junit.Assert;
import org.junit.Test;

public class ReadCachePrefixTest {

    @Test
    public void buildTest() {
        Assert.assertEquals("QUERY.5", ReadCachePrefix.addQueryPrefix(5));
        Assert.assertEquals("X.QUERY.5", ReadCachePrefix.addQueryPrefix("X", 5));
        Assert.assertEquals("FILTER.7", ReadCachePrefix.addFilterPrefix(7));
        Assert.assertEquals("Y.FILTER.7", ReadCachePrefix.addFilterPrefix("Y", 7));
    }
}
