package cn.edu.thu.tsfiledb.query.aggregation.impl;

import cn.edu.thu.tsfile.common.utils.Binary;
import cn.edu.thu.tsfile.file.metadata.enums.TSDataType;
import cn.edu.thu.tsfile.format.Digest;
import cn.edu.thu.tsfile.format.PageHeader;
import cn.edu.thu.tsfile.timeseries.filter.utils.DigestForFilter;
import cn.edu.thu.tsfile.timeseries.read.query.DynamicOneColumnData;
import cn.edu.thu.tsfiledb.query.aggregation.AggregateFunction;


public class MinValueAggrFunc extends AggregateFunction{

	private boolean hasSetValue = false;
	
	public MinValueAggrFunc(TSDataType dataType) {
		super("MIN_VALUE", dataType);
	}
	
	@Override
	public boolean calculateValueFromPageHeader(PageHeader pageHeader) {
		Digest pageDigest = pageHeader.data_page_header.getDigest();
		DigestForFilter digest = new DigestForFilter(pageDigest.min, pageDigest.max, dataType);
		Comparable<?> minv = digest.getMinValue();
		if(!hasSetValue){
			result.data.putAnObject(minv);
			hasSetValue = true;
		}else{
			Comparable<?> v = result.data.getAnObject(0);
			if(compare(v, minv) > 0){
				result.data.setAnObject(0, minv);
			}
		}
		return true;
	}

	@Override
	public void calculateValueFromDataInThisPage(DynamicOneColumnData dataInThisPage) {
		if(dataInThisPage.length == 0){
			return;
		}
		Comparable<?> minv = getMinValue(dataInThisPage);
		if(!hasSetValue){
			result.data.putAnObject(minv);
			hasSetValue = true;
		}else{
			Comparable<?> v = result.data.getAnObject(0);
			if(compare(v, minv) > 0){
				result.data.setAnObject(0, minv);
			}
		}
	}
	
	public Comparable<?> getMinValue(DynamicOneColumnData dataInThisPage){
		Comparable<?> v = dataInThisPage.getAnObject(0);
		for(int i = 1; i < dataInThisPage.length ; i++){
			Comparable<?> nextV = dataInThisPage.getAnObject(i);
			if(compare(v, nextV) > 0){
				v = nextV;
			}
		}
		return v;
	}
	
	public int compare(Comparable<?> o1, Comparable<?> o2){
		switch(dataType){
		case BOOLEAN:
			return ((Boolean)o1).compareTo((Boolean)o2);
		case INT32:
			return ((Integer)o1).compareTo((Integer)o2);
		case INT64:
			return ((Long)o1).compareTo((Long)o2);
		case FLOAT:
			return ((Float)o1).compareTo((Float)o2);
		case DOUBLE:
			return ((Double)o1).compareTo((Double)o2);
		case FIXED_LEN_BYTE_ARRAY:
			return ((Binary)o1).compareTo((Binary)o2);
		default:
			return 0;
		}
	}

}
