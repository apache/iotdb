package cn.edu.tsinghua.tsfile.file.metadata;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import cn.edu.tsinghua.tsfile.common.utils.TsRandomAccessFileWriter;
import cn.edu.tsinghua.tsfile.file.metadata.utils.TestHelper;
import cn.edu.tsinghua.tsfile.file.metadata.utils.Utils;
import cn.edu.tsinghua.tsfile.file.utils.ReadWriteThriftFormatUtils;
import cn.edu.tsinghua.tsfile.format.RowGroupBlockMetaData;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class RowGroupBlockMetadataBigTest {
	private static int deviceNum = 100;
	private static int sensorNum = 1000;
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

	private static RowGroupMetaData createSimpleRowGroupMetaDataInTSF() throws UnsupportedEncodingException {
		RowGroupMetaData metaData = new RowGroupMetaData(RowGroupMetaDataTest.DELTA_OBJECT_UID,
				RowGroupMetaDataTest.MAX_NUM_ROWS, RowGroupMetaDataTest.TOTAL_BYTE_SIZE, new ArrayList<>(),
				RowGroupMetaDataTest.DELTA_OBJECT_TYPE);
		metaData.setPath(RowGroupMetaDataTest.FILE_PATH);
		for (int i = 0; i < sensorNum; i++) {
			metaData.addTimeSeriesChunkMetaData(TestHelper.createSimpleTimeSeriesChunkMetaDataInTSF());
		}
		return metaData;
	}

	@Test
	public void test() throws IOException {
		System.out.println("-------------Start Metadata big data test------------");
		long startTime = System.currentTimeMillis();
		List<RowGroupMetaData> rowGroupMetaDatas = new ArrayList<>();
		for (int i = 0; i < deviceNum; i++) {
			rowGroupMetaDatas.add(createSimpleRowGroupMetaDataInTSF());
		}
		TsRowGroupBlockMetaData metaData = new TsRowGroupBlockMetaData(rowGroupMetaDatas);
		metaData.setDeltaObjectID(DELTA_OBJECT_UID);
		System.out.println("1: create Metadata " + (System.currentTimeMillis() - startTime)+"ms");

		startTime = System.currentTimeMillis();
		RowGroupBlockMetaData metaDataInThrift = metaData.convertToThrift();
		System.out.println("2: covernet to Thrift " + (System.currentTimeMillis() - startTime)+"ms");

		Utils.isRowGroupBlockMetadataEqual(metaData, metaDataInThrift);
		
		startTime = System.currentTimeMillis();
		File file = new File(PATH);
		if (file.exists())
			file.delete();
		TsRandomAccessFileWriter out = new TsRandomAccessFileWriter(file, "rw");
		ReadWriteThriftFormatUtils.writeRowGroupBlockMetadata(metaDataInThrift, out.getOutputStream());
		out.close();
		System.out.println("3: write to File" + (System.currentTimeMillis() - startTime)+"ms");

		FileInputStream fis = new FileInputStream(file);
		System.out.println("file size: " + fis.available());
		fis.close();
		
		FileInputStream fis2 = new FileInputStream(new File(PATH));
		startTime = System.currentTimeMillis();
		RowGroupBlockMetaData metaDataInThrift2 = ReadWriteThriftFormatUtils.readRowGroupBlockMetaData(fis2);
		System.out.println("4: read from File" + (System.currentTimeMillis() - startTime)+"ms");
	    Utils.isRowGroupBlockMetadataEqual(metaData, metaDataInThrift2);
	    System.out.println("-------------End Metadata big data test------------");
	}

//	public static void main(String[] args) throws IOException {
		// long startTime = System.currentTimeMillis();
		// File file = new File(PATH);
		// RandomAccessOutputStream outputStream = new
		// RandomAccessOutputStream(file, "rw");
		// byte[] b = new byte[20*1024*1024];
		// outputStream.write(b);
		// outputStream.close();
		// System.out.println("3: "+(System.currentTimeMillis()-startTime));
		// FileInputStream fis = new FileInputStream(file);
		// System.out.println("file size: "+fis.available());
		// fis.close();
		//
		// if (file.exists())
		// file.delete();
//	}

}
