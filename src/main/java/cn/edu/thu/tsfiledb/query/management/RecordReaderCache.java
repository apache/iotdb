package cn.edu.thu.tsfiledb.query.management;

import java.util.HashMap;

import cn.edu.thu.tsfiledb.query.reader.RecordReader;

public class RecordReaderCache {
	
	ThreadLocal<HashMap<String,RecordReader>> cache = new ThreadLocal<>();
	
	
	public boolean containsRecordReader(String deltaObjectUID, String measurementID){
		checkCacheInitialized();
		return cache.get().containsKey(getKey(deltaObjectUID, measurementID));
	}
	
	public RecordReader get(String deltaObjectUID, String measurementID){
		checkCacheInitialized();
		return cache.get().get(getKey(deltaObjectUID, measurementID));
	}
	
	public void put(String deltaObjectUID, String measurementID, RecordReader recordReader){
		checkCacheInitialized();
		cache.get().put(getKey(deltaObjectUID, measurementID), recordReader);
	}
	
	public void clear(){
		cache.remove();
	}
	
	private String getKey(String deltaObjectUID, String measurementID){
		return deltaObjectUID + "#" + measurementID;
	}
	
	private void checkCacheInitialized(){
		if(cache.get() == null){
			cache.set(new HashMap<>());
		}
	}
}
