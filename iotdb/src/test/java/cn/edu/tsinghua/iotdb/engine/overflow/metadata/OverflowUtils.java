package cn.edu.tsinghua.iotdb.engine.overflow.metadata;

import cn.edu.tsinghua.tsfile.file.metadata.ChunkMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.TsDeviceMetadataIndex;
import cn.edu.tsinghua.tsfile.file.metadata.TsFileMetaData;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class OverflowUtils {

	/**
	 * when one of A and B is Null, A != B, so test case fails.
	 *
	 * @param objectA
	 * @param objectB
	 * @param name
	 * @return false - A and B both are NULL, so we do not need to check whether their members are equal
	 *         true - A and B both are not NULL, so we need to check their members
	 */
	public static boolean isTwoObjectsNotNULL(Object objectA, Object objectB, String name) {
		if ((objectA == null) && (objectB == null))
			return false;
		if ((objectA == null) ^ (objectB == null))
			fail(String.format("one of %s is null", name));
		return true;
	}

	public static void isOFSeriesListMetadataEqual(OFSeriesListMetadata ofSeriesListMetadata1, OFSeriesListMetadata ofSeriesListMetadata2){
		if(isTwoObjectsNotNULL(ofSeriesListMetadata1,ofSeriesListMetadata2,"OFSeriesListMetadata")){
			if(isTwoObjectsNotNULL(ofSeriesListMetadata1.getMeasurementId(),
							ofSeriesListMetadata2.getMeasurementId(),"measurement id")){
				assertTrue(ofSeriesListMetadata1.getMeasurementId().equals(ofSeriesListMetadata2.getMeasurementId()));
			}
			assertEquals(ofSeriesListMetadata1.getMetaDatas().size(),ofSeriesListMetadata2.getMetaDatas().size());
			List<ChunkMetaData> chunkMetaDataList1 = ofSeriesListMetadata1.getMetaDatas();
			List<ChunkMetaData> chunkMetaDataList2 = ofSeriesListMetadata2.getMetaDatas();
			for(int i = 0;i<chunkMetaDataList1.size();i++){
				isTimeSeriesChunkMetadataEqual(chunkMetaDataList1.get(i),chunkMetaDataList2.get(i));
			}
		}
	}

	public static void isTimeSeriesChunkMetadataEqual(ChunkMetaData metadata1,
													  ChunkMetaData metadata2) {
		if (isTwoObjectsNotNULL(metadata1, metadata2, "ChunkMetaData")) {
			if (isTwoObjectsNotNULL(metadata1.getMeasurementUID(), metadata2.getMeasurementUID(),
					"sensorUID")) {
				assertTrue(metadata1.getMeasurementUID().equals(metadata2.getMeasurementUID()));
			}
			assertTrue(metadata1.getOffsetOfChunkHeader() == metadata2.getOffsetOfChunkHeader());
			assertTrue(metadata1.getNumOfPoints() == metadata2.getNumOfPoints());
			assertTrue(metadata1.getStartTime() == metadata2.getStartTime());
			assertTrue(metadata1.getEndTime() == metadata2.getEndTime());
			if (isTwoObjectsNotNULL(metadata1.getDigest(), metadata2.getDigest(), "digest")) {
				isMapBufferEqual(metadata1.getDigest().getStatistics(), metadata2.getDigest().getStatistics(), "statistics");
			}
		}
	}

	public static void isMapBufferEqual(Map<String, ByteBuffer> mapA, Map<String, ByteBuffer> mapB, String name) {
		if ((mapA == null) ^ (mapB == null)) {
			System.out.println("error");
			fail(String.format("one of %s is null", name));
		}
		if ((mapA != null) && (mapB != null)) {
			if (mapA.size() != mapB.size()) {
				fail(String.format("%s size is different", name));
			}
			for (String key : mapB.keySet()) {
				ByteBuffer b = mapB.get(key);
				ByteBuffer a = mapA.get(key);
				assertTrue(b.equals(a));
			}
		}
	}

	public static void isOFRowGroupListMetadataEqual(OFRowGroupListMetadata ofRowGroupListMetadata1, OFRowGroupListMetadata ofRowGroupListMetadata2){
		if(isTwoObjectsNotNULL(ofRowGroupListMetadata1,
						ofRowGroupListMetadata2,"OFRowGroupListMetadata")){
			assertTrue(ofRowGroupListMetadata1.getdeviceId().equals(ofRowGroupListMetadata2.getdeviceId()));
			List<OFSeriesListMetadata> list1 = ofRowGroupListMetadata1.getSeriesList();
			List<OFSeriesListMetadata> list2 = ofRowGroupListMetadata2.getSeriesList();
			assertEquals(list1.size(),list2.size());
			for(int i = 0;i<list1.size();i++){
				isOFSeriesListMetadataEqual(list1.get(i),list2.get(i));
			}
		}
	}

	public static void isOFFileMetadataEqual(OFFileMetadata ofFileMetadata1, OFFileMetadata ofFileMetadata2){
		if(isTwoObjectsNotNULL(ofFileMetadata1,ofFileMetadata2,"OFFileMetadata")){
			assertEquals(ofFileMetadata1.getLastFooterOffset(),ofFileMetadata2.getLastFooterOffset());
			List<OFRowGroupListMetadata> list1 = ofFileMetadata1.getRowGroupLists();
			List<OFRowGroupListMetadata> list2 = ofFileMetadata2.getRowGroupLists();
			assertNotNull(list1);
			assertNotNull(list2);
			assertEquals(list1.size(),list2.size());
			for(int i = 0;i<list1.size();i++){
				isOFRowGroupListMetadataEqual(list1.get(i),list2.get(i));
			}
		}
	}

}
