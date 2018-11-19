package cn.edu.tsinghua.tsfile.timeseries.utils;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import cn.edu.tsinghua.tsfile.timeseries.constant.TimeseriesTestConstant;
import cn.edu.tsinghua.tsfile.timeseries.utils.FileUtils.Unit;

/**
 * 
 * @author kangrong
 *
 */
public class FileUtilsTest {

    @Test
    public void testConvertUnit() {
        long kb = 3 * 1024;
        long mb = kb * 1024;
        long gb = mb * 1024;
        assertEquals(3.0 * 1024, FileUtils.transformUnit(kb, Unit.B),
                TimeseriesTestConstant.double_min_delta);
        assertEquals(3, FileUtils.transformUnit(kb, Unit.KB),
                TimeseriesTestConstant.double_min_delta);

        assertEquals(3, FileUtils.transformUnit(mb, Unit.MB),
                TimeseriesTestConstant.double_min_delta);
        assertEquals(3, FileUtils.transformUnit(gb, Unit.GB),
                TimeseriesTestConstant.double_min_delta);
    }

    @Test
    public void testConvertToByte() {
        assertEquals(3l, (long) FileUtils.transformUnitToByte(3, Unit.B));
        assertEquals(3l * 1024, (long) FileUtils.transformUnitToByte(3, Unit.KB));
        assertEquals(3l * 1024 * 1024, (long) FileUtils.transformUnitToByte(3, Unit.MB));
        assertEquals(3l * 1024 * 1024 * 1024, (long) FileUtils.transformUnitToByte(3, Unit.GB));
    }

//    @Deprecated
//    public void testGetLocalFileByte() {
//        String fileName = "src/test/resources/test_schema.json";
//        assertEquals(843.0, FileUtils.getLocalFileByte(fileName, Unit.B),
//                TimeseriesTestConstant.double_min_delta);
//        assertEquals(0.82, FileUtils.getLocalFileByte(fileName, Unit.KB),
//                TimeseriesTestConstant.double_min_delta);
//    }
}
