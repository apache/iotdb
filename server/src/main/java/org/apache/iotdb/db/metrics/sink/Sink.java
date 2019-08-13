package org.apache.iotdb.db.metrics.sink;

public interface Sink {
	
	public void start();
	
	public void stop();
	
	public void report();

}
