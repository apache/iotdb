package cn.edu.tsinghua.iotdb.engine.bufferwrite;

import cn.edu.tsinghua.iotdb.engine.memtable.IMemTable;
import cn.edu.tsinghua.iotdb.engine.memtable.MemTableFlushUtil;
import cn.edu.tsinghua.iotdb.engine.memtable.MemTableTestUtils;
import cn.edu.tsinghua.iotdb.engine.memtable.PrimitiveMemTable;
import cn.edu.tsinghua.iotdb.utils.EnvironmentUtils;
import cn.edu.tsinghua.tsfile.file.metadata.ChunkGroupMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.ChunkMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.TsDeviceMetadata;
import cn.edu.tsinghua.tsfile.file.metadata.TsFileMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSEncoding;
import cn.edu.tsinghua.tsfile.read.TsFileSequenceReader;
import cn.edu.tsinghua.tsfile.utils.BytesUtils;
import cn.edu.tsinghua.tsfile.utils.Pair;
import cn.edu.tsinghua.tsfile.write.schema.FileSchema;
import cn.edu.tsinghua.tsfile.write.schema.MeasurementSchema;
import cn.edu.tsinghua.tsfile.write.writer.TsFileIOWriter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class RestorableTsFileIOWriterTest {

	private RestorableTsFileIOWriter writer;
	private String processorName = "processor";
	private String insertPath = "insertfile";
	private String restorePath = insertPath + ".restore";

	@Before
	public void setUp() throws Exception {
		
	}

	@After
	public void tearDown() throws Exception {
		EnvironmentUtils.cleanDir(insertPath);
		EnvironmentUtils.cleanDir(restorePath);
	}

	@Test
	public void testInitResource() throws IOException {
		writer = new RestorableTsFileIOWriter(processorName, insertPath);

		Pair<Long, List<ChunkGroupMetaData>> pair = writer.readRestoreInfo();
		assertEquals(true, new File(restorePath).exists());

		assertEquals(TsFileIOWriter.magicStringBytes.length, (long) pair.left);
		assertEquals(0, pair.right.size());
		writer.endFile(new FileSchema());
		deleteInsertFile();
		assertEquals(false, new File(restorePath).exists());
	}
	
	@Test
	public void testAbnormalRecover() throws IOException{
		writer = new RestorableTsFileIOWriter(processorName, insertPath);
		File insertFile = new File(insertPath);
		File restoreFile = new File(restorePath);
		FileOutputStream fileOutputStream = new FileOutputStream(insertFile);
		// mkdir
		fileOutputStream.write(new byte[400]);
		fileOutputStream.close();
		assertEquals(true, insertFile.exists());
		assertEquals(true, restoreFile.exists());
		assertEquals(400, insertFile.length());
		writer.endFile(new FileSchema());

		FileOutputStream out = new FileOutputStream(new File(restorePath));
		// write tsfile position using byte[8] which is present one long
		writeRestoreFile(out, 2);
		writeRestoreFile(out, 3);
		byte[] lastPositionBytes = BytesUtils.longToBytes(200);
		out.write(lastPositionBytes);
		out.close();
		writer = new RestorableTsFileIOWriter(processorName, insertPath);

		assertEquals(true, insertFile.exists());
		assertEquals(200, insertFile.length());
		assertEquals(insertPath, writer.getInsertFilePath());
		assertEquals(restorePath, writer.getRestoreFilePath());
		writer.endFile(new FileSchema());
		deleteInsertFile();
	}

	@Test
	public void testRecover() throws IOException {
		File insertFile = new File(insertPath);
		FileOutputStream fileOutputStream = new FileOutputStream(insertFile);
		fileOutputStream.write(new byte[200]);
		fileOutputStream.close();

		File restoreFile = new File(insertPath+".restore");
		FileOutputStream out = new FileOutputStream(new File(restorePath));
		// write tsfile position using byte[8] which is present one long
		writeRestoreFile(out, 2);
		writeRestoreFile(out, 3);
		byte[] lastPositionBytes = BytesUtils.longToBytes(200);
		out.write(lastPositionBytes);
		out.close();

		writer = new RestorableTsFileIOWriter(processorName, insertPath);
		//writer.endFile(new FileSchema());


		assertEquals(true, insertFile.exists());
		assertEquals(true, restoreFile.exists());


		RestorableTsFileIOWriter tempbufferwriteResource = new RestorableTsFileIOWriter(processorName, insertPath);

		assertEquals(true, insertFile.exists());
		assertEquals(200, insertFile.length());
		assertEquals(insertPath, tempbufferwriteResource.getInsertFilePath());
		assertEquals(restorePath, tempbufferwriteResource.getRestoreFilePath());

		tempbufferwriteResource.endFile(new FileSchema());
		writer.endFile(new FileSchema());
		deleteInsertFile();
	}

	@Test
	public void testWriteAndRecover() throws IOException {
		writer = new RestorableTsFileIOWriter(processorName, insertPath);
		FileSchema schema = new FileSchema();
		schema.registerMeasurement(new MeasurementSchema("s1", TSDataType.INT32, TSEncoding.RLE));
		schema.registerMeasurement(new MeasurementSchema("s2", TSDataType.INT32, TSEncoding.RLE));

		//TsFileWriter fileWriter = new TsFileWriter();
		PrimitiveMemTable memTable = new PrimitiveMemTable();
		memTable.write("d1", "s1", TSDataType.INT32, 1, "1");
		memTable.write("d1", "s1", TSDataType.INT32, 2, "1");
		memTable.write("d1", "s2", TSDataType.INT32, 1, "1");
		memTable.write("d1", "s2", TSDataType.INT32, 3, "1");
		memTable.write("d2", "s2", TSDataType.INT32, 2, "1");
		memTable.write("d2", "s2", TSDataType.INT32, 4, "1");
		MemTableFlushUtil.flushMemTable(schema, writer, memTable);
		writer.flush();
		writer.appendMetadata();
		writer.getOutput().close();

		//recover
		writer = new RestorableTsFileIOWriter(processorName, insertPath);
		writer.endFile(schema);

		TsFileSequenceReader reader = new TsFileSequenceReader(insertPath);
		TsFileMetaData metaData = reader.readFileMetadata();
		assertEquals(2, metaData.getDeviceMap().size());
		List<ChunkGroupMetaData> chunkGroups = reader.readTsDeviceMetaData(metaData.getDeviceMap().get("d1")).getChunkGroups();
		assertEquals(1, chunkGroups.size());

		List<ChunkMetaData> chunks = chunkGroups.get(0).getChunkMetaDataList();
		assertEquals(2, chunks.size());
		//d1.s1
		assertEquals(chunks.get(0).getStartTime(), 1);
		assertEquals(chunks.get(0).getEndTime(), 2);
		assertEquals(chunks.get(0).getNumOfPoints(), 2);
		//d1.s2
		assertEquals(chunks.get(1).getStartTime(), 1);
		assertEquals(chunks.get(1).getEndTime(), 3);
		assertEquals(chunks.get(1).getNumOfPoints(), 2);

		chunkGroups = reader.readTsDeviceMetaData(metaData.getDeviceMap().get("d2")).getChunkGroups();
		assertEquals(1, chunkGroups.size());
		chunks = chunkGroups.get(0).getChunkMetaDataList();
		assertEquals(1, chunks.size());
		//da.s2
		assertEquals(chunks.get(0).getStartTime(), 2);
		assertEquals(chunks.get(0).getEndTime(), 4);
		assertEquals(chunks.get(0).getNumOfPoints(), 2);

		reader.close();
	}

	@Test
	public void testFlushAndGetMetadata() throws IOException {
		writer = new RestorableTsFileIOWriter(processorName, insertPath);

		assertEquals(0, writer.getMetadatas(MemTableTestUtils.deviceId0,
				MemTableTestUtils.measurementId0, MemTableTestUtils.dataType0).size());

		IMemTable memTable = new PrimitiveMemTable();
		MemTableTestUtils.produceData(memTable, 10, 100, MemTableTestUtils.deviceId0,
				MemTableTestUtils.measurementId0, MemTableTestUtils.dataType0);

		MemTableFlushUtil.flushMemTable(MemTableTestUtils.getFileSchema(), writer, memTable);
		writer.flush();

		assertEquals(0, writer.getMetadatas(MemTableTestUtils.deviceId0,
				MemTableTestUtils.measurementId0, MemTableTestUtils.dataType0).size());
		writer.appendMetadata();
		assertEquals(1, writer.getMetadatas(MemTableTestUtils.deviceId0,
				MemTableTestUtils.measurementId0, MemTableTestUtils.dataType0).size());
		MemTableTestUtils.produceData(memTable, 200, 300, MemTableTestUtils.deviceId0,
				MemTableTestUtils.measurementId0, MemTableTestUtils.dataType0);
		writer.appendMetadata();
		assertEquals(1, writer.getMetadatas(MemTableTestUtils.deviceId0,
				MemTableTestUtils.measurementId0, MemTableTestUtils.dataType0).size());

		writer.endFile(MemTableTestUtils.getFileSchema());
		deleteInsertFile();
	}

	private void writeRestoreFile(OutputStream out, int metadataNum) throws IOException {
		TsDeviceMetadata tsDeviceMetadata = new TsDeviceMetadata();
		List<ChunkGroupMetaData> appendRowGroupMetaDatas = new ArrayList<>();
		for (int i = 0; i < metadataNum; i++) {
			appendRowGroupMetaDatas.add(new ChunkGroupMetaData("d1", new ArrayList<>()));
		}
		tsDeviceMetadata.setChunkGroupMetadataList(appendRowGroupMetaDatas);
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		tsDeviceMetadata.serializeTo(baos);
		// write metadata size using int
		int metadataSize = baos.size();
		out.write(BytesUtils.intToBytes(metadataSize));
		// write metadata
		out.write(baos.toByteArray());
	}

	private void deleteInsertFile(){
		try {
			Files.delete(Paths.get(insertPath));
		} catch (IOException e) {
			fail(e.getMessage());
		}
	}
}
