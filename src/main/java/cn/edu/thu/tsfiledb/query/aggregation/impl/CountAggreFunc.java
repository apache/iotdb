package cn.edu.thu.tsfiledb.query.aggregation.impl;

import cn.edu.thu.tsfile.file.metadata.enums.TSDataType;
import cn.edu.thu.tsfile.format.PageHeader;
import cn.edu.thu.tsfile.timeseries.read.query.DynamicOneColumnData;
import cn.edu.thu.tsfiledb.query.aggregation.AggregateFunction;

public class CountAggreFunc extends AggregateFunction{

	public CountAggreFunc() {
		super("COUNT", TSDataType.INT64);
		result.data.putLong(0);
	}

	@Override
	public boolean calculateValueFromPageHeader(PageHeader pageHeader) {
		long preValue = result.data.getLong(0);
		preValue += pageHeader.data_page_header.num_rows;
		result.data.setLong(0, preValue);
		return true;
	}

	@Override
	public void calculateValueFromDataInThisPage(DynamicOneColumnData dataInThisPage) {
		long preValue = result.data.getLong(0);
		preValue += dataInThisPage.length;
		result.data.setLong(0, preValue);
	}
}
