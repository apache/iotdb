package cn.edu.thu.tsfiledb.engine.utils;

/**
 * This class is used to represent the state of flush. It's can be used in the
 * bufferwrite flush{@code BufferWriteProcessor} and overflow
 * flush{@code OverFlowProcessor}.
 * 
 * @author liukun
 */
public class FlushState {

	private volatile boolean isFlushing;

	public FlushState() {
		this.isFlushing = false;
	}

	public boolean isFlushing() {
		return isFlushing;
	}

	public void setFlushing() {
		this.isFlushing = true;
	}

	public void setUnFlushing() {
		this.isFlushing = false;
	}

}
