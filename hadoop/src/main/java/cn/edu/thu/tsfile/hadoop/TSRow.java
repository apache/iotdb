package cn.edu.thu.tsfile.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import cn.edu.tsinghua.tsfile.timeseries.write.record.TSRecord;

public class TSRow implements Writable {
	
	private TSRecord row;
	
	public TSRow(TSRecord row){
		
		this.row = row;
	}
	
	public TSRecord getRow(){
		return row;
	}

	@Override
	public void write(DataOutput out) throws IOException {

		throw new IOException("Not support");
	}

	@Override
	public void readFields(DataInput in) throws IOException {

		throw new IOException("Not support");
	}
}
