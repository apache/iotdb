package cn.edu.tsinghua.iotdb.engine.utils;

/**
 * This class is used to represent the state of flush. It's can be used in the
 * bufferwrite flush{@code BufferWriteProcessor} and overflow
 * flush{@code OverFlowProcessor}.
 * 
 * @author liukun
 */
public class FlushStatus {

	/*
	0910: 了解volatile(https://www.cnblogs.com/dolphin0520/p/3920373.html)，
	考虑用锁保证多线程原子性？
	 */
	private volatile boolean isFlushing;

	public FlushStatus() {
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
