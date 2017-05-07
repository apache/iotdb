package cn.edu.thu.tsfiledb.query.management;

import java.util.HashMap;

import cn.edu.thu.tsfiledb.engine.filenode.FileNodeManager;
import cn.edu.thu.tsfiledb.exception.ProcessorException;

public class ReadLockManager {
	
	private static ReadLockManager instance = new ReadLockManager();
	FileNodeManager fileNodeManager = FileNodeManager.getInstance(); 
	ThreadLocal<HashMap<String,Integer>> locksMap = new ThreadLocal<>();
	
	private ReadLockManager(){
		
	}
	
	public int lock(String deltaObjectUID, String measurementID) throws ProcessorException{
		checkLocksMap();
		String key = getKey(deltaObjectUID, measurementID);
		int token;
		if(!locksMap.get().containsKey(key)){
			token = fileNodeManager.beginQuery(deltaObjectUID, measurementID);
			locksMap.get().put(key, token);
		}else{
			token = locksMap.get().get(key);
		}
		return token;
	}
	
	public void unlockForSubQuery(String deltaObjectUID, String measurementID
			, int token) throws ProcessorException{
		fileNodeManager.removeNewQueryOnePassLock(deltaObjectUID, measurementID, token);
	}
	
	public void unlockForQuery(String deltaObjectUID, String measurementID
			, int token) throws ProcessorException{
		fileNodeManager.removeMultiPassQueryLock(deltaObjectUID, measurementID, token);
	}
	
	public void unlockForOneRequest() throws NotConsistentException, ProcessorException{
		if(locksMap.get() == null){
			throw new NotConsistentException("There is no locks in last request");
		}
		HashMap<String,Integer> locks = locksMap.get();
		for(String key : locks.keySet()){
			String[] names = splitKey(key);
			unlockForQuery(names[0], names[1], locks.get(key));
		}
		locksMap.remove();
	}
	
	public String getKey(String deltaObjectUID, String measurementID){
		return deltaObjectUID + "#" + measurementID; 
	}
	
	public String[] splitKey(String key){
		return key.split("#");
	}
	
	public static ReadLockManager getInstance(){
		if(instance == null){
			instance = new ReadLockManager();
		}
		return instance;
	}
	
	public void checkLocksMap(){
		if(locksMap.get() == null){
			locksMap.set(new HashMap<String, Integer>());
		}
	}
}
