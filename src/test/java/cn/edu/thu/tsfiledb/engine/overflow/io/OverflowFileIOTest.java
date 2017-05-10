package cn.edu.thu.tsfiledb.engine.overflow.io;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import cn.edu.thu.tsfile.file.metadata.TimeSeriesChunkMetaData;
import cn.edu.thu.tsfile.file.metadata.enums.CompressionTypeName;
import cn.edu.thu.tsfile.file.metadata.enums.TSDataType;
import cn.edu.thu.tsfile.file.metadata.statistics.LongStatistics;
import cn.edu.thu.tsfiledb.engine.overflow.metadata.OFFileMetadata;
import cn.edu.thu.tsfiledb.engine.overflow.metadata.OFRowGroupListMetadata;
import cn.edu.thu.tsfiledb.engine.overflow.metadata.OFSeriesListMetadata;
import cn.edu.thu.tsfiledb.engine.bufferwrite.FileNodeConstants;
import cn.edu.thu.tsfiledb.engine.overflow.io.OverflowFileIO;
import cn.edu.thu.tsfiledb.engine.overflow.io.OverflowReadWriter;

public class OverflowFileIOTest {

	// construct the overflowreadwriteio
	// construct the overflow file io from zero offset
	// construct the seriesstart, data and seriesend
	// get the offilemetadata

	private String filePath = "overflowfileiotest";
	private String mergeFilePath = filePath + ".merge";

	private OverflowReadWriter raf = null;

	private OverflowFileIO overflowFileIO = null;

	private OFFileMetadata ofFileMetadata = null;
	private OFRowGroupListMetadata ofRowGroupMetadata = null;
	private OFSeriesListMetadata ofSeriesMetadata = null;
	private TimeSeriesChunkMetaData timeseriesChunkMetadata = null;

	private int valuecount = 10;
	private int numOfBytes = 20;
	private String[] measurementIds = { "s0", "s1", "s2", "s3", "s4", "s5" };
	private TSDataType[] datatypes = { TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE,
			TSDataType.BOOLEAN, TSDataType.BYTE_ARRAY };

	private int numofrowgroup = 10;
	private int numofseries = measurementIds.length;
	private int numofserieschunk = 4;
	private int numoffile = 3;
	private long lastFileOffset = 0;

	@Before
	public void setUp() throws Exception {
		EngineTestHelper.delete(filePath);
		EngineTestHelper.delete(mergeFilePath);
		ofSeriesMetadata = new OFSeriesListMetadata();
		ofRowGroupMetadata = new OFRowGroupListMetadata();
		ofFileMetadata = new OFFileMetadata();
	}

	@After
	public void tearDown() throws Exception {
		EngineTestHelper.delete(filePath);
		EngineTestHelper.delete(mergeFilePath);
	}

	@Test
	public void test() throws IOException {

		try {
			raf = new OverflowReadWriter(filePath);
			overflowFileIO = new OverflowFileIO(raf, filePath, 0);
		} catch (IOException e) {
			e.printStackTrace();
			fail("Construct the overflowreadwrite error or construct the overflowfile io error");
		}

		// one overflow file
		ofRowGroupMetadata = new OFRowGroupListMetadata();
		for (int k = 0; k < numoffile; k++) {
			ofFileMetadata = new OFFileMetadata();
			// one row group
			for (int m = 0; m < numofrowgroup; m++) {
				ofRowGroupMetadata = new OFRowGroupListMetadata();
				ofRowGroupMetadata.setDeltaObjectId("deltaObjectId" + m);
				// one overflow series
				for (int i = 0; i < numofseries; i++) {
					ofSeriesMetadata = new OFSeriesListMetadata();
					// one series chunk
					for (int j = 1; j <= numofserieschunk; j++) {
						// new one TimeSeriesChunkMetaData
						long begin;
						try {
							begin = overflowFileIO.getTail();
							overflowFileIO.startSeries(valuecount, measurementIds[i], CompressionTypeName.UNCOMPRESSED,
									datatypes[i], new LongStatistics());
							// write data use raf
							byte[] buff = new byte[numOfBytes];
							raf.write(buff);
							assertEquals((j + i * numofserieschunk + m * (numofserieschunk * numofseries)) * 20
									+ lastFileOffset, overflowFileIO.getTail());
							long end = overflowFileIO.getTail();
							byte[] readBuff = new byte[numOfBytes];
							// read data from begin to the end
							overflowFileIO.getSeriesChunkBytes(numOfBytes, begin).read(readBuff);
							assertEquals(overflowFileIO.getPos(), overflowFileIO.getTail());
							assertArrayEquals(buff, readBuff);
							// end one TimeSeriesChunkMetaData
							timeseriesChunkMetadata = overflowFileIO.endSeries(end - begin);
						} catch (IOException e) {
							e.printStackTrace();
							fail("construct the TimeSeriesChunkMetaData failed, the reason is " + e.getMessage());
						}
						ofSeriesMetadata.setMeasurementId(measurementIds[i]);
						ofSeriesMetadata.addSeriesMetaData(timeseriesChunkMetadata);
					}
					ofRowGroupMetadata.addSeriesListMetaData(ofSeriesMetadata);
				}
				ofFileMetadata.addRowGroupListMetaData(ofRowGroupMetadata);
			}
			System.out.println(String.format("This file data length is %d", overflowFileIO.getTail() - lastFileOffset));
			ofFileMetadata.setLastFooterOffset(lastFileOffset);
			lastFileOffset = overflowFileIO.endOverflow(ofFileMetadata);
			// the second offilemetadata
			raf = new OverflowReadWriter(filePath);
			overflowFileIO = new OverflowFileIO(raf, filePath, lastFileOffset);
			overflowFileIO.toTail();
			System.out.println("End the num of overflow file is " + numoffile);
		}
		// test switch work to merge
		overflowFileIO.switchFileIOWorkingToMerge();
		// check the mrege file and the length
		String mergePath = filePath + FileNodeConstants.PATH_SEPARATOR + FileNodeConstants.MREGE_EXTENSION;
		File mergeFile = new File(mergePath);
		assertEquals(true, mergeFile.exists());
		assertEquals(lastFileOffset, mergeFile.length());
		assertEquals(true, new File(filePath).exists());
		assertEquals(0, new File(filePath).length());
		assertEquals(true, overflowFileIO.getOverflowIOForMerge()!=null);
		// test swithc merge to work
		overflowFileIO.switchFileIOMergeToWorking();
		// check merge file
		assertEquals(false, mergeFile.exists());
		assertEquals(null, overflowFileIO.getOverflowIOForMerge());
	}

}
