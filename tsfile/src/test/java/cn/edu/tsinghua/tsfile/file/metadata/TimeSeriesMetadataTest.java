package cn.edu.tsinghua.tsfile.file.metadata;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import cn.edu.tsinghua.tsfile.file.metadata.utils.TestHelper;
import cn.edu.tsinghua.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TimeSeriesMetadataTest {
    public static final String measurementUID = "sensor01";
    public static final int typeLength = 1024;
    final String PATH = "target/outputTimeSeries.tsfile";

    @Before
    public void setUp() {}

    @After
    public void tearDown() {
        File file = new File(PATH);
        if (file.exists())
            file.delete();
    }

    @Test
    public void testWriteIntoFile() throws IOException {
        MeasurementSchema measurementSchema = TestHelper.createSimpleMeasurementSchema();
        serialized(measurementSchema);
        MeasurementSchema readMetadata = deSerialized();
        measurementSchema.equals(readMetadata);
        serialized(readMetadata);
    }

    private MeasurementSchema deSerialized() {
        FileInputStream fis = null;
        MeasurementSchema metaData = null;
        try {
            fis = new FileInputStream(new File(PATH));
            metaData = MeasurementSchema.deserializeFrom(fis);
            return metaData;
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (fis != null) {
                try {
                    fis.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return metaData;
    }


    private void serialized(MeasurementSchema metaData){
        File file = new File(PATH);
        if (file.exists()){
            file.delete();
        }
        FileOutputStream fos = null;
        try {
            fos = new FileOutputStream(file);
            metaData.serializeTo(fos);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            if(fos!=null){
                try {
                    fos.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
