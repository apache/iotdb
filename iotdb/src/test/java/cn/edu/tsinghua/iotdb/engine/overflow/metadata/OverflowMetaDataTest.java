package cn.edu.tsinghua.iotdb.engine.overflow.metadata;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import cn.edu.tsinghua.iotdb.engine.overflow.utils.TSFileMetaDataConverter;
import cn.edu.tsinghua.tsfile.common.utils.TsRandomAccessFileWriter;
import cn.edu.tsinghua.tsfile.file.metadata.TimeSeriesChunkMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.enums.CompressionTypeName;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSChunkType;
import cn.edu.tsinghua.tsfile.format.CompressionType;
import cn.edu.tsinghua.tsfile.format.TimeSeriesChunkType;



/**
 * Unit test for the metadata of overflow. {@code OFSeriesListMetadata} convert
 * the thrift {@code cn.edu.thu.tsfile.format.OFSeriesListMetadata}
 * {@code OFRowGroupListMetadata} convert the thrift
 * {@code cn.edu.thu.tsfile.format.OFRowGroupListMetadata}
 * {@code OFFileMetadata} convert the thrift
 * {@code cn.edu.thu.tsfile.format.OFFileMetadata} Convert the overflow file
 * metadata to overflow thrift metadata, write to file stream. Read from the
 * file stream, convert the overflow thrift metadata to the overflow file
 * metadata.
 * 
 * @author liukun
 *
 */
public class OverflowMetaDataTest {

	// data
	private final String DELTA_OBJECT_UID = "delta-3312";
	private final String MEASUREMENT_UID = "sensor231";
	private final long FILE_OFFSET = 2313424242L;
	private final long MAX_NUM_ROWS = 423432425L;
	private final long TOTAL_BYTE_SIZE = 432453453L;
	private final long DATA_PAGE_OFFSET = 42354334L;
	private final long DICTIONARY_PAGE_OFFSET = 23434543L;
	private final long INDEX_PAGE_OFFSET = 34243453L;
	private final int LAST_FOOTER_OFFSET = 3423432;
	private final String PATH = "target/OFFileMetaData.overflow";

	// series chunk metadata
	private TimeSeriesChunkMetaData tsfTimeSeriesChunkMetaData;
	private cn.edu.tsinghua.tsfile.format.TimeSeriesChunkMetaData thriftTimeSeriesChunkMetaData;
	// of series list
	private OFSeriesListMetadata tsfOFSeriesListMetadata;
	private cn.edu.tsinghua.iotdb.engine.overflow.thrift.OFSeriesListMetadata thriftOFSeriesListMetadata;
	// of row group list
	private OFRowGroupListMetadata tsfOFRowGroupListMetadata;
	private cn.edu.tsinghua.iotdb.engine.overflow.thrift.OFRowGroupListMetadata thriftOFRowGroupListMetadata;
	// of file list
	private OFFileMetadata tsfOFFileMetadata;
	private cn.edu.tsinghua.iotdb.engine.overflow.thrift.OFFileMetadata thriftOFFileMetadata;

	// converter
	private TSFileMetaDataConverter converter;

	@Before
	public void setUp() throws Exception {

	}

	@After
	public void tearDown() throws Exception {
		File file = new File(PATH);
		if (file.exists()) {
			file.delete();
		}
	}

	@Test
	public void OFSeriesListMetadataTest() {
		// of to thrift
		tsfOFSeriesListMetadata = new OFSeriesListMetadata();
		tsfOFSeriesListMetadata.setMeasurementId(MEASUREMENT_UID);
		for (CompressionTypeName compressionTypeName : CompressionTypeName.values()) {
			for (TSChunkType chunkType : TSChunkType.values()) {
				tsfTimeSeriesChunkMetaData = new TimeSeriesChunkMetaData(MEASUREMENT_UID, chunkType, FILE_OFFSET,
						compressionTypeName);
				tsfTimeSeriesChunkMetaData.setNumRows(MAX_NUM_ROWS);
				tsfTimeSeriesChunkMetaData.setTotalByteSize(TOTAL_BYTE_SIZE);

				tsfTimeSeriesChunkMetaData.setJsonMetaData(TestHelper.getJSONArray());

				tsfTimeSeriesChunkMetaData.setDataPageOffset(DATA_PAGE_OFFSET);
				tsfTimeSeriesChunkMetaData.setDictionaryPageOffset(DICTIONARY_PAGE_OFFSET);
				tsfTimeSeriesChunkMetaData.setIndexPageOffset(INDEX_PAGE_OFFSET);
				tsfOFSeriesListMetadata.addSeriesMetaData(tsfTimeSeriesChunkMetaData);
			}
		}
		Utils.isOFSeriesListMetaDataEqual(tsfOFSeriesListMetadata, tsfOFSeriesListMetadata.convertToThrift());

		// thrift to of
		thriftOFSeriesListMetadata = new cn.edu.tsinghua.iotdb.engine.overflow.thrift.OFSeriesListMetadata();
		thriftOFSeriesListMetadata.setMeasurement_id(MEASUREMENT_UID);
		for (CompressionType compressionType : CompressionType.values()) {
			for (TimeSeriesChunkType chunkType : TimeSeriesChunkType.values()) {
				thriftTimeSeriesChunkMetaData = new cn.edu.tsinghua.tsfile.format.TimeSeriesChunkMetaData(
						MEASUREMENT_UID, chunkType, FILE_OFFSET, compressionType);
				thriftTimeSeriesChunkMetaData.setNum_rows(MAX_NUM_ROWS);
				thriftTimeSeriesChunkMetaData.setTotal_byte_size(TOTAL_BYTE_SIZE);
				thriftTimeSeriesChunkMetaData.setJson_metadata(TestHelper.getJSONArray());
				thriftTimeSeriesChunkMetaData.setData_page_offset(DATA_PAGE_OFFSET);
				thriftTimeSeriesChunkMetaData.setDictionary_page_offset(DICTIONARY_PAGE_OFFSET);
				thriftTimeSeriesChunkMetaData.setIndex_page_offset(INDEX_PAGE_OFFSET);

				thriftOFSeriesListMetadata.addToTsc_metadata(thriftTimeSeriesChunkMetaData);
			}
		}
		tsfOFSeriesListMetadata = new OFSeriesListMetadata();
		tsfOFSeriesListMetadata.convertToTSF(thriftOFSeriesListMetadata);
		Utils.isOFSeriesListMetaDataEqual(tsfOFSeriesListMetadata, thriftOFSeriesListMetadata);
	}

	@Test
	public void OFRowGroupListMetadataTest() {
		// of to thrift
		tsfOFRowGroupListMetadata = new OFRowGroupListMetadata();
		tsfOFRowGroupListMetadata.setDeltaObjectId(DELTA_OBJECT_UID);
		int size = 5;
		while (size > 0) {
			size--;
			tsfOFSeriesListMetadata = new OFSeriesListMetadata();
			tsfOFSeriesListMetadata.setMeasurementId(MEASUREMENT_UID);
			for (CompressionTypeName compressionTypeName : CompressionTypeName.values()) {
				for (TSChunkType chunkType : TSChunkType.values()) {
					tsfTimeSeriesChunkMetaData = new TimeSeriesChunkMetaData(MEASUREMENT_UID, chunkType, FILE_OFFSET,
							compressionTypeName);
					tsfTimeSeriesChunkMetaData.setNumRows(MAX_NUM_ROWS);
					tsfTimeSeriesChunkMetaData.setTotalByteSize(TOTAL_BYTE_SIZE);

					tsfTimeSeriesChunkMetaData.setJsonMetaData(TestHelper.getJSONArray());

					tsfTimeSeriesChunkMetaData.setDataPageOffset(DATA_PAGE_OFFSET);
					tsfTimeSeriesChunkMetaData.setDictionaryPageOffset(DICTIONARY_PAGE_OFFSET);
					tsfTimeSeriesChunkMetaData.setIndexPageOffset(INDEX_PAGE_OFFSET);
					tsfOFSeriesListMetadata.addSeriesMetaData(tsfTimeSeriesChunkMetaData);
				}
			}
			tsfOFRowGroupListMetadata.addSeriesListMetaData(tsfOFSeriesListMetadata);
		}
		Utils.isOFRowGroupListMetaDataEqual(tsfOFRowGroupListMetadata, tsfOFRowGroupListMetadata.convertToThrift());

		// thrift to of

		thriftOFRowGroupListMetadata = new cn.edu.tsinghua.iotdb.engine.overflow.thrift.OFRowGroupListMetadata();
		thriftOFRowGroupListMetadata.setDeltaObject_id(DELTA_OBJECT_UID);
		size = 5;
		while (size > 0) {
			size--;
			thriftOFSeriesListMetadata = new cn.edu.tsinghua.iotdb.engine.overflow.thrift.OFSeriesListMetadata();
			thriftOFSeriesListMetadata.setMeasurement_id(MEASUREMENT_UID);
			for (CompressionType compressionType : CompressionType.values()) {
				for (TimeSeriesChunkType chunkType : TimeSeriesChunkType.values()) {
					thriftTimeSeriesChunkMetaData = new cn.edu.tsinghua.tsfile.format.TimeSeriesChunkMetaData(
							MEASUREMENT_UID, chunkType, FILE_OFFSET, compressionType);
					thriftTimeSeriesChunkMetaData.setNum_rows(MAX_NUM_ROWS);
					thriftTimeSeriesChunkMetaData.setTotal_byte_size(TOTAL_BYTE_SIZE);
					thriftTimeSeriesChunkMetaData.setJson_metadata(TestHelper.getJSONArray());
					thriftTimeSeriesChunkMetaData.setData_page_offset(DATA_PAGE_OFFSET);
					thriftTimeSeriesChunkMetaData.setDictionary_page_offset(DICTIONARY_PAGE_OFFSET);
					thriftTimeSeriesChunkMetaData.setIndex_page_offset(INDEX_PAGE_OFFSET);

					thriftOFSeriesListMetadata.addToTsc_metadata(thriftTimeSeriesChunkMetaData);
				}
			}
			thriftOFRowGroupListMetadata.addToMeasurement_metadata(thriftOFSeriesListMetadata);
		}
		tsfOFRowGroupListMetadata = new OFRowGroupListMetadata();
		tsfOFRowGroupListMetadata.convertToTSF(thriftOFRowGroupListMetadata);
		Utils.isOFRowGroupListMetaDataEqual(tsfOFRowGroupListMetadata, thriftOFRowGroupListMetadata);
	}

	@Test
	public void OFFileMetaDataTest() {

		converter = new TSFileMetaDataConverter();

		tsfOFFileMetadata = new OFFileMetadata();
		tsfOFFileMetadata.setLastFooterOffset(LAST_FOOTER_OFFSET);
		int count = 5;
		while (count > 0) {
			count--;
			tsfOFRowGroupListMetadata = new OFRowGroupListMetadata();
			tsfOFRowGroupListMetadata.setDeltaObjectId(DELTA_OBJECT_UID);
			int size = 5;
			while (size > 0) {
				size--;
				tsfOFSeriesListMetadata = new OFSeriesListMetadata();
				tsfOFSeriesListMetadata.setMeasurementId(MEASUREMENT_UID);
				for (CompressionTypeName compressionTypeName : CompressionTypeName.values()) {
					for (TSChunkType chunkType : TSChunkType.values()) {
						tsfTimeSeriesChunkMetaData = new TimeSeriesChunkMetaData(MEASUREMENT_UID, chunkType,
								FILE_OFFSET, compressionTypeName);
						tsfTimeSeriesChunkMetaData.setNumRows(MAX_NUM_ROWS);
						tsfTimeSeriesChunkMetaData.setTotalByteSize(TOTAL_BYTE_SIZE);

						tsfTimeSeriesChunkMetaData.setJsonMetaData(TestHelper.getJSONArray());

						tsfTimeSeriesChunkMetaData.setDataPageOffset(DATA_PAGE_OFFSET);
						tsfTimeSeriesChunkMetaData.setDictionaryPageOffset(DICTIONARY_PAGE_OFFSET);
						tsfTimeSeriesChunkMetaData.setIndexPageOffset(INDEX_PAGE_OFFSET);
						tsfOFSeriesListMetadata.addSeriesMetaData(tsfTimeSeriesChunkMetaData);
					}
				}
				tsfOFRowGroupListMetadata.addSeriesListMetaData(tsfOFSeriesListMetadata);
			}
			tsfOFFileMetadata.addRowGroupListMetaData(tsfOFRowGroupListMetadata);
		}
		Utils.isOFFileMetaDataEqual(tsfOFFileMetadata, tsfOFFileMetadata.convertToThrift());
		Utils.isOFFileMetaDataEqual(tsfOFFileMetadata, converter.toThriftOFFileMetadata(0, tsfOFFileMetadata));

		// thrift to of
		thriftOFFileMetadata = new cn.edu.tsinghua.iotdb.engine.overflow.thrift.OFFileMetadata();
		thriftOFFileMetadata.setLast_footer_offset(LAST_FOOTER_OFFSET);
		count = 5;
		while (count > 0) {
			count--;
			thriftOFRowGroupListMetadata = new cn.edu.tsinghua.iotdb.engine.overflow.thrift.OFRowGroupListMetadata();
			thriftOFRowGroupListMetadata.setDeltaObject_id(DELTA_OBJECT_UID);
			int size = 5;
			while (size > 0) {
				size--;
				thriftOFSeriesListMetadata = new cn.edu.tsinghua.iotdb.engine.overflow.thrift.OFSeriesListMetadata();
				thriftOFSeriesListMetadata.setMeasurement_id(MEASUREMENT_UID);
				for (CompressionType compressionType : CompressionType.values()) {
					for (TimeSeriesChunkType chunkType : TimeSeriesChunkType.values()) {
						thriftTimeSeriesChunkMetaData = new cn.edu.tsinghua.tsfile.format.TimeSeriesChunkMetaData(
								MEASUREMENT_UID, chunkType, FILE_OFFSET, compressionType);
						thriftTimeSeriesChunkMetaData.setNum_rows(MAX_NUM_ROWS);
						thriftTimeSeriesChunkMetaData.setTotal_byte_size(TOTAL_BYTE_SIZE);
						thriftTimeSeriesChunkMetaData.setJson_metadata(TestHelper.getJSONArray());
						thriftTimeSeriesChunkMetaData.setData_page_offset(DATA_PAGE_OFFSET);
						thriftTimeSeriesChunkMetaData.setDictionary_page_offset(DICTIONARY_PAGE_OFFSET);
						thriftTimeSeriesChunkMetaData.setIndex_page_offset(INDEX_PAGE_OFFSET);

						thriftOFSeriesListMetadata.addToTsc_metadata(thriftTimeSeriesChunkMetaData);
					}
				}
				thriftOFRowGroupListMetadata.addToMeasurement_metadata(thriftOFSeriesListMetadata);
			}
			thriftOFFileMetadata.addToDeltaObject_metadata(thriftOFRowGroupListMetadata);
		}
		tsfOFFileMetadata = new OFFileMetadata();
		tsfOFFileMetadata.convertToTSF(thriftOFFileMetadata);
		Utils.isOFFileMetaDataEqual(tsfOFFileMetadata, thriftOFFileMetadata);
		Utils.isOFFileMetaDataEqual(converter.toOFFileMetadata(thriftOFFileMetadata), thriftOFFileMetadata);
	}

	@Test
	public void OFFileThriftFileTest() throws IOException {
		// offilemetadata flush to file
		tsfOFFileMetadata = new OFFileMetadata();
		tsfOFFileMetadata.setLastFooterOffset(LAST_FOOTER_OFFSET);
		int count = 5;
		while (count > 0) {
			count--;
			tsfOFRowGroupListMetadata = new OFRowGroupListMetadata();
			tsfOFRowGroupListMetadata.setDeltaObjectId(DELTA_OBJECT_UID);
			int size = 5;
			while (size > 0) {
				size--;
				tsfOFSeriesListMetadata = new OFSeriesListMetadata();
				tsfOFSeriesListMetadata.setMeasurementId(MEASUREMENT_UID);
				for (CompressionTypeName compressionTypeName : CompressionTypeName.values()) {
					for (TSChunkType chunkType : TSChunkType.values()) {
						tsfTimeSeriesChunkMetaData = new TimeSeriesChunkMetaData(MEASUREMENT_UID, chunkType,
								FILE_OFFSET, compressionTypeName);
						tsfTimeSeriesChunkMetaData.setNumRows(MAX_NUM_ROWS);
						tsfTimeSeriesChunkMetaData.setTotalByteSize(TOTAL_BYTE_SIZE);

						tsfTimeSeriesChunkMetaData.setJsonMetaData(TestHelper.getJSONArray());

						tsfTimeSeriesChunkMetaData.setDataPageOffset(DATA_PAGE_OFFSET);
						tsfTimeSeriesChunkMetaData.setDictionaryPageOffset(DICTIONARY_PAGE_OFFSET);
						tsfTimeSeriesChunkMetaData.setIndexPageOffset(INDEX_PAGE_OFFSET);
						tsfOFSeriesListMetadata.addSeriesMetaData(tsfTimeSeriesChunkMetaData);
					}
				}
				tsfOFRowGroupListMetadata.addSeriesListMetaData(tsfOFSeriesListMetadata);
			}
			tsfOFFileMetadata.addRowGroupListMetaData(tsfOFRowGroupListMetadata);
		}

		File file = new File(PATH);
		if (file.exists()) {
			file.delete();
		}

		FileOutputStream fos = new FileOutputStream(file);
		TsRandomAccessFileWriter out = new TsRandomAccessFileWriter(file, "rw");
		Utils.write(tsfOFFileMetadata.convertToThrift(), out.getOutputStream());

		out.close();
		fos.close();
		// thriftfilemeta read from file
		FileInputStream fis = new FileInputStream(new File(PATH));
		Utils.isOFFileMetaDataEqual(tsfOFFileMetadata,
				Utils.read(fis, new cn.edu.tsinghua.iotdb.engine.overflow.thrift.OFFileMetadata()));

	}

}