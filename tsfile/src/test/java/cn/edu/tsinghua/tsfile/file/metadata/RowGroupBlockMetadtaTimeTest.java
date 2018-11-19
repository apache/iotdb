package cn.edu.tsinghua.tsfile.file.metadata;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.*;

import cn.edu.tsinghua.tsfile.common.utils.TsRandomAccessFileWriter;
import cn.edu.tsinghua.tsfile.file.metadata.utils.TestHelper;
import cn.edu.tsinghua.tsfile.file.utils.ReadWriteThriftFormatUtils;
import cn.edu.tsinghua.tsfile.format.RowGroupBlockMetaData;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class RowGroupBlockMetadtaTimeTest {
    private static int deviceNum = 20;
    private static int sensorNum = 50;
    private static String PATH = "target/test-big.ksn";
    public static final String DELTA_OBJECT_UID = "delta-3312";

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
        File file = new File(PATH);
        if (file.exists())
            file.delete();
    }

    private static RowGroupMetaData createSimpleRowGroupMetaDataInTSF(String delta_object_uid) throws UnsupportedEncodingException {
        RowGroupMetaData metaData = new RowGroupMetaData(delta_object_uid,
                RowGroupMetaDataTest.MAX_NUM_ROWS, RowGroupMetaDataTest.TOTAL_BYTE_SIZE, new ArrayList<>(),
                RowGroupMetaDataTest.DELTA_OBJECT_TYPE);
        metaData.setPath(RowGroupMetaDataTest.FILE_PATH);
        for (int i = 0; i < sensorNum; i++) {
            metaData.addTimeSeriesChunkMetaData(TestHelper.createSimpleTimeSeriesChunkMetaDataInTSF());
        }
        return metaData;
    }

    //@Test
    public void test1() throws IOException {
        System.out.println();
        test_one_io(10);
        test_one_io(50);
        test_one_io(100);
        test_one_io(10000);
    }

    //@Test
    public void test2() throws IOException {
        System.out.println();
        test_multi_io(10);
        test_multi_io(50);
        test_multi_io(100);
        test_multi_io(10000);
    }

    public void test_multi_io(int delta_object_num) throws IOException {
        System.out.println("-------------Start Metadata multi_io test_object" + delta_object_num + "------------");
        long startTime = System.currentTimeMillis();
        Map<String, TsRowGroupBlockMetaData> tsRowGroupBlockMetaDataMap = new HashMap<>();
        List<String> delta_object_list = new ArrayList<>();
        for (int i = 0; i < delta_object_num; i++) {
            delta_object_list.add("delta-" + i);
        }
        List<RowGroupMetaData> rowGroupMetaDatas;
        for (int i = 0; i < delta_object_num; i++) {
            String delta_object_id = delta_object_list.get(i);
            rowGroupMetaDatas = new ArrayList<>();
            for (int j = 0; j < deviceNum; j++) {
                rowGroupMetaDatas.add(createSimpleRowGroupMetaDataInTSF(delta_object_id));
            }
            TsRowGroupBlockMetaData tsRowGroupBlockMetaData = new TsRowGroupBlockMetaData(rowGroupMetaDatas);
            tsRowGroupBlockMetaData.setDeltaObjectID(delta_object_id);
            tsRowGroupBlockMetaDataMap.put(delta_object_id, tsRowGroupBlockMetaData);
        }

        System.out.println("1: create Metadata " + (System.currentTimeMillis() - startTime)+"ms");

        File file = new File(PATH);
        if (file.exists())
            file.delete();
        startTime = System.currentTimeMillis();
        TsRandomAccessFileWriter out = new TsRandomAccessFileWriter(file, "rw");
        long offset;
        long offset_index;
        int metadataBlockSize;
        long start;
        long end;
        String current_deltaobject;
        TsRowGroupBlockMetaData current_tsRowGroupBlockMetaData;
        Long start_index = out.getPos();
        Iterator<Map.Entry<String, TsRowGroupBlockMetaData>> iterator = tsRowGroupBlockMetaDataMap.entrySet().iterator();
        while (iterator.hasNext()) {
            start = Long.MAX_VALUE;
            end = Long.MIN_VALUE;
            Map.Entry<String, TsRowGroupBlockMetaData> entry = iterator.next();
            current_deltaobject = entry.getKey();
            current_tsRowGroupBlockMetaData = entry.getValue();
			for (RowGroupMetaData rowGroupMetaData : current_tsRowGroupBlockMetaData.getRowGroups()) {
				for (TimeSeriesChunkMetaData timeSeriesChunkMetaData : rowGroupMetaData.getTimeSeriesChunkMetaDataList()) {
					start = Long.min(start, timeSeriesChunkMetaData.getTInTimeSeriesChunkMetaData().getStartTime());
					end = Long.max(end, timeSeriesChunkMetaData.getTInTimeSeriesChunkMetaData().getEndTime());
				}
			}
            //flush tsRowGroupBlockMetaDatas in order
            RowGroupBlockMetaData rowGroupBlockMetaData = current_tsRowGroupBlockMetaData.convertToThrift();
            ReadWriteThriftFormatUtils.writeRowGroupBlockMetadata(rowGroupBlockMetaData, out.getOutputStream());
        }
        Long end_index = out.getPos();
        out.close();
        System.out.println("2:" + (end_index - start_index) + " bytes write to File: " + (System.currentTimeMillis() - startTime)+"ms");
        System.out.println("-------------End Metadata multi_io test------------");
        System.out.println();
    }

    public void test_one_io(int delta_object_num) throws IOException {
        System.out.println("-------------Start Metadata one_io test delta_object" + delta_object_num + "------------");
        long startTime = System.currentTimeMillis();
        String delta_object_id = "test";
        List<RowGroupMetaData> rowGroupMetaDatas = new ArrayList<>();
        for (int i = 0; i < delta_object_num; i++) {
            for (int j = 0; j < deviceNum; j++) {
                rowGroupMetaDatas.add(createSimpleRowGroupMetaDataInTSF(delta_object_id));
            }
        }
        TsRowGroupBlockMetaData tsRowGroupBlockMetaData = new TsRowGroupBlockMetaData(rowGroupMetaDatas);
        tsRowGroupBlockMetaData.setDeltaObjectID(delta_object_id);
        System.out.println("1: create Metadata " + (System.currentTimeMillis() - startTime)+"ms");
        File file = new File(PATH);
        if (file.exists())
            file.delete();
        startTime = System.currentTimeMillis();
        TsRandomAccessFileWriter out = new TsRandomAccessFileWriter(file, "rw");
        String current_deltaobject;
        TsRowGroupBlockMetaData current_tsRowGroupBlockMetaData;
        Long start_index = out.getPos();
        ReadWriteThriftFormatUtils.writeRowGroupBlockMetadata(
                tsRowGroupBlockMetaData.convertToThrift(), out.getOutputStream()
        );
        Long end_index = out.getPos();
        out.close();
        System.out.println("2:" + (end_index - start_index) + " bytes write to File: " + (System.currentTimeMillis() - startTime)+"ms");
        System.out.println("-------------End Metadata one_io test------------");
        System.out.println();
    }
}
