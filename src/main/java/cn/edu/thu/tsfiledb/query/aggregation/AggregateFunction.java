package cn.edu.thu.tsfiledb.query.aggregation;

import cn.edu.thu.tsfile.file.metadata.enums.TSDataType;
import cn.edu.thu.tsfile.format.PageHeader;
import cn.edu.thu.tsfile.timeseries.read.query.DynamicOneColumnData;

public abstract class AggregateFunction {
	
	public String name;
	public AggregationResult result;
	public TSDataType dataType; 
	
	public AggregateFunction(String name, TSDataType dataType){
		this.name = name;
		this.dataType = dataType;
		result = new AggregationResult(dataType);
		result.data.putTime(0);
	}
	
	/**
	 * 
	 * @param pageHeader
	 * @return False represents that the result can not be calculated from pageHeader
	 */
	public boolean calculateFromPageHeader(PageHeader pageHeader){
		boolean ret = calculateValueFromPageHeader(pageHeader);
		if(ret){
			addCount(pageHeader);
		}
		return ret;
	}
	
	public abstract boolean calculateValueFromPageHeader(PageHeader pageHeader);
	
	public void calculateFromDataInThisPage(DynamicOneColumnData dataInThisPage){
		calculateValueFromDataInThisPage(dataInThisPage);
		addCount(dataInThisPage);
	}
	
	/**
	 * Calculate the value according to all data in this page
	 * @param dataInThisPage
	 */
	public abstract void calculateValueFromDataInThisPage(DynamicOneColumnData dataInThisPage);
	
	/**
	 * calculate the count of add records 
	 * @param pageHeader
	 */
	public void addCount(PageHeader pageHeader){
		long count = result.data.getTime(0) + pageHeader.data_page_header.num_rows;
		result.data.setTime(0, count);
	}
	
	public void addCount(DynamicOneColumnData dataInThisPage){
		long count = result.data.getTime(0) + dataInThisPage.length;
		result.data.setTime(0, count);
	}
	
}
