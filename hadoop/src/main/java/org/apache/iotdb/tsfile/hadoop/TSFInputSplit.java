package org.apache.iotdb.tsfile.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import org.apache.iotdb.tsfile.file.metadata.RowGroupMetaData;
import org.apache.iotdb.tsfile.file.metadata.TsRowGroupBlockMetaData;
import org.apache.iotdb.tsfile.file.utils.ReadWriteThriftFormatUtils;
import org.apache.iotdb.tsfile.format.RowGroupBlockMetaData;

/**
 * This is tsfile <code>InputSplit</code>.<br>
 * Each <code>InputSplit</code> will be processed by individual
 * <code>Mapper</code> task.
 * 
 * @author liukun
 */
public class TSFInputSplit extends InputSplit implements Writable {

	private Path path;
	private int numOfDeviceRowGroup;
	private List<RowGroupMetaData> deviceRowGroupMetaDataList;
	private long start;
	private long length;
	private String[] hosts;

	public TSFInputSplit() {

	}

	/**
	 * @param path
	 * @param deviceRowGroupMetaDataList
	 * @param start
	 * @param length
	 * @param hosts
	 */
	public TSFInputSplit(Path path, List<RowGroupMetaData> deviceRowGroupMetaDataList, long start, long length,
			String[] hosts) {
		this.path = path;
		this.deviceRowGroupMetaDataList = deviceRowGroupMetaDataList;
		this.numOfDeviceRowGroup = deviceRowGroupMetaDataList.size();
		this.start = start;
		this.length = length;
		this.hosts = hosts;
	}

	/**
	 * @return the path
	 */
	public Path getPath() {
		return path;
	}

	/**
	 * @param path
	 *            the path to set
	 */
	public void setPath(Path path) {
		this.path = path;
	}

	/**
	 * @return the numOfDeviceRowGroup
	 */
	public int getNumOfDeviceRowGroup() {
		return numOfDeviceRowGroup;
	}

	/**
	 * @param numOfDeviceRowGroup
	 *            the numOfDeviceRowGroup to set
	 */
	public void setNumOfDeviceRowGroup(int numOfDeviceRowGroup) {
		this.numOfDeviceRowGroup = numOfDeviceRowGroup;
	}

	/**
	 * @return the deviceRowGroupMetaDataList
	 */
	public List<RowGroupMetaData> getDeviceRowGroupMetaDataList() {
		return deviceRowGroupMetaDataList;
	}

	/**
	 * @param deviceRowGroupMetaDataList
	 *            the deviceRowGroupMetaDataList to set
	 */
	public void setDeviceRowGroupMetaDataList(List<RowGroupMetaData> deviceRowGroupMetaDataList) {
		this.deviceRowGroupMetaDataList = deviceRowGroupMetaDataList;
	}

	/**
	 * @return the start
	 */
	public long getStart() {
		return start;
	}

	/**
	 * @param start
	 *            the start to set
	 */
	public void setStart(long start) {
		this.start = start;
	}

	@Override
	public long getLength() throws IOException, InterruptedException {
		return this.length;
	}

	@Override
	public String[] getLocations() throws IOException, InterruptedException {
		return this.hosts;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(path.toString());
		out.writeLong(start);
		out.writeLong(length);
		out.writeInt(hosts.length);
		for (int i = 0; i < hosts.length; i++) {
			String string = hosts[i];
			out.writeUTF(string);
		}
		out.writeInt(numOfDeviceRowGroup);
		RowGroupBlockMetaData rowGroupBlockMetaData = new TsRowGroupBlockMetaData(deviceRowGroupMetaDataList)
				.convertToThrift();
		ReadWriteThriftFormatUtils.writeRowGroupBlockMetadata(rowGroupBlockMetaData, (OutputStream) out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		path = new Path(in.readUTF());
		this.start = in.readLong();
		this.length = in.readLong();
		int len = in.readInt();
		this.hosts = new String[len];
		for (int i = 0; i < len; i++) {
			hosts[i] = in.readUTF();
		}
		this.numOfDeviceRowGroup = in.readInt();
		TsRowGroupBlockMetaData tsRowGroupBlockMetaData = new TsRowGroupBlockMetaData();
		tsRowGroupBlockMetaData.convertToTSF(ReadWriteThriftFormatUtils.readRowGroupBlockMetaData((InputStream) in));
		deviceRowGroupMetaDataList = tsRowGroupBlockMetaData.getRowGroups();
	}

	@Override
	public String toString() {
		return "TSFInputSplit [path=" + path + ", numOfDeviceGroup=" + numOfDeviceRowGroup
				+ ", deviceRowGroupMetaDataList=" + deviceRowGroupMetaDataList + ", start=" + start + ", length="
				+ length + ", hosts=" + Arrays.toString(hosts) + "]";
	}

}
