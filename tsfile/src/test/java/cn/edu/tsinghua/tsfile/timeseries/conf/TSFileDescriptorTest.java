package cn.edu.tsinghua.tsfile.timeseries.conf;

import cn.edu.tsinghua.tsfile.common.conf.TSFileConfig;

import static org.junit.Assert.assertEquals;
/**
 * Note that this test case should run separately.
 * @author XuYi
 */
public class TSFileDescriptorTest {
    public int groupSizeInBytePre = 128 * 1024 * 1024;
    public int pageSizeInBytePre = 1024 * 1024;
    public int maxNumberOfPointsInPagePre = 1024 * 1024;
    public String timeSeriesDataTypePre = "INT64";
    public int maxStringLengthPre = 128;
    public int floatPrecisionPre = 2;
    public String timeSeriesEncoderPre = "TS_2DIFF";
    public String valueEncoderPre = "PALIN";
    public String compressorPre = "UNCOMPRESSED";
    public TSFileConfig config;
    
//    @Before
//    public void before() {
//	config = TSFileDescriptor.getInstance().getConfig();
//    }
//
//    @After
//    public void after() {
//	config.groupSizeInByte = groupSizeInBytePre;
//	config.pageSizeInByte = pageSizeInBytePre;
//	config.maxNumberOfPointsInPage = maxNumberOfPointsInPagePre;
//	config.timeSeriesDataType = timeSeriesDataTypePre;
//	config.maxStringLength = maxStringLengthPre;
//	config.floatPrecision = floatPrecisionPre;
//	config.timeSeriesEncoder = timeSeriesEncoderPre;
//	config.valueEncoder = valueEncoderPre;
//	config.compressor = compressorPre;
//    }
//
//    @Test
//    public void testLoadProp() {
//	assertEquals(config.groupSizeInByte, 123456789);
//	assertEquals(config.pageSizeInByte, 123456);
//	assertEquals(config.maxNumberOfPointsInPage, 12345);
//	assertEquals(config.timeSeriesDataType, "INT32");
//	assertEquals(config.maxStringLength, 64);
//	assertEquals(config.floatPrecision, 5);
//	assertEquals(config.timeSeriesEncoder, "RLE");
//	assertEquals(config.valueEncoder, "RLE");
//	assertEquals(config.compressor, "SNAPPY");
//    }

}
