package cn.edu.tsinghua.iotdb.engine.overflow.metadata;


import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.tsfile.file.metadata.converter.IConverter;

/**
 * This is the metadata for overflow file. More information see
 * {@code com.corp.delta.tsfile.format.OFFileMetadata}
 * 
 * @author liukun
 *
 */
public class OFFileMetadata implements IConverter<cn.edu.tsinghua.iotdb.engine.overflow.thrift.OFFileMetadata> {
	private static final Logger LOGGER = LoggerFactory.getLogger(OFFileMetadata.class);

	private long lastFooterOffset;
	private List<OFRowGroupListMetadata> rowGroupLists;

	public OFFileMetadata() {
	}

	public OFFileMetadata(long lastFooterOffset, List<OFRowGroupListMetadata> rowGroupLists) {
		this.lastFooterOffset = lastFooterOffset;
		this.rowGroupLists = rowGroupLists;
	}

	/**
	 * add OFRowGroupListMetadata to list
	 * 
	 * @param rowGroupListMetadata
	 * @return void
	 */
	public void addRowGroupListMetaData(OFRowGroupListMetadata rowGroupListMetadata) {
		if (rowGroupLists == null) {
			rowGroupLists = new ArrayList<OFRowGroupListMetadata>();
		}
		rowGroupLists.add(rowGroupListMetadata);
	}

	public List<OFRowGroupListMetadata> getMetaDatas() {
		return rowGroupLists == null ? null : Collections.unmodifiableList(rowGroupLists);
	}

	@Override
	public cn.edu.tsinghua.iotdb.engine.overflow.thrift.OFFileMetadata convertToThrift() {
		try {
			List<cn.edu.tsinghua.iotdb.engine.overflow.thrift.OFRowGroupListMetadata> ofRowGroupList = null;
			if (rowGroupLists != null) {
				ofRowGroupList = new ArrayList<cn.edu.tsinghua.iotdb.engine.overflow.thrift.OFRowGroupListMetadata>();
				for (OFRowGroupListMetadata rowGroupListMetaData : rowGroupLists) {
					ofRowGroupList.add(rowGroupListMetaData.convertToThrift());
				}
			}
			cn.edu.tsinghua.iotdb.engine.overflow.thrift.OFFileMetadata metaData = new cn.edu.tsinghua.iotdb.engine.overflow.thrift.OFFileMetadata(
					ofRowGroupList, lastFooterOffset);
			return metaData;
		} catch (Exception e) {
			LOGGER.error("failed to convert OFFileMetadata from TSF to thrift, RowGroup lists metadata:{}", toString(),
					e);
			throw e;
		}
	}

	
	@Override
	public void convertToTSF(cn.edu.tsinghua.iotdb.engine.overflow.thrift.OFFileMetadata metadata) {
		try {
			lastFooterOffset = metadata.getLast_footer_offset();
			List<cn.edu.tsinghua.iotdb.engine.overflow.thrift.OFRowGroupListMetadata> list = metadata.getDeltaObject_metadata();
			if (list == null) {
				rowGroupLists = null;
			} else {
				if (rowGroupLists == null) {
					rowGroupLists = new ArrayList<OFRowGroupListMetadata>();
				}
				rowGroupLists.clear();
				for (cn.edu.tsinghua.iotdb.engine.overflow.thrift.OFRowGroupListMetadata formatSeries : list) {
					OFRowGroupListMetadata ofRowgroupListMetaData = new OFRowGroupListMetadata();
					ofRowgroupListMetaData.convertToTSF(formatSeries);
					rowGroupLists.add(ofRowgroupListMetaData);
				}
			}
		} catch (Exception e) {
			LOGGER.error("Failed to convert OFFileMetadata from thrift to TSF, RowGroup list metadata:{}",
					metadata.toString(), e);
			throw e;
		}
	}


	@Override
	public String toString() {
		return String.format("OFFileMetadata{ last offset: %d, RowGroupLists: %s }", lastFooterOffset,
				rowGroupLists.toString());
	}

	public long getLastFooterOffset() {
		return lastFooterOffset;
	}

	public void setLastFooterOffset(long lastFooterOffset) {
		this.lastFooterOffset = lastFooterOffset;
	}

	public List<OFRowGroupListMetadata> getRowGroupLists() {
		return this.rowGroupLists;
	}
}
