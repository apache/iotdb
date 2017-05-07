package cn.edu.thu.tsfiledb.engine.bufferwrite;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfile.timeseries.write.exception.WriteProcessException;
import cn.edu.thu.tsfiledb.exception.ErrorDebugException;
import cn.edu.thu.tsfiledb.exception.PathErrorException;
import cn.edu.thu.tsfiledb.exception.ProcessorException;
import cn.edu.thu.tsfiledb.metadata.MManager;

/**
 * <p>
 * LRUManager manage a list of processor: overflow/filenode/bufferwrite
 * processor<br>
 *
 * @author kangrong
 * @author liukun
 */
public abstract class LRUManager<T extends LRUProcessor> {
	private static Logger LOG = LoggerFactory.getLogger(LRUManager.class);
	private LinkedList<T> processorLRUList;
	private Map<String, T> processorMap;
	private final int maxLRUNodeNumber;
	protected final MManager mManager;
	protected final String normalDataDir;
	private static final long removeCheckInterval = 1000;

	protected LRUManager(int maxLRUNumber, MManager mManager, String normalDataDir) {
		this.mManager = mManager;
		this.maxLRUNodeNumber = maxLRUNumber;
		LOG.info("The max of LRUProcessor number of {} is {}", this.getClass().getSimpleName(), maxLRUNumber);
		processorLRUList = new LinkedList<>();
		processorMap = new HashMap<>();

		if (normalDataDir.charAt(normalDataDir.length() - 1) != File.separatorChar)
			normalDataDir += File.separatorChar;
		this.normalDataDir = normalDataDir;
		File dir = new File(normalDataDir);
		if (dir.mkdirs()) {
			LOG.info("{} dir home is not exists, create it", this.getClass().getSimpleName());
		}
	}

	/**
	 * <p>
	 * Get the data directory
	 * 
	 * @return data directory
	 */
	public String getNormalDataDir() {
		return normalDataDir;
	}

	/**
	 * <p>
	 * Get processor just using the metadata path
	 * 
	 * @param path
	 * @param isWriteLock
	 * @return LRUProcessor
	 * @throws ProcessorException
	 * @throws IOException
	 */
	public T getProcessorWithDeltaObjectIdByLRU(String path, boolean isWriteLock)
			throws ProcessorException, IOException, WriteProcessException {
		return getProcessorWithDeltaObjectIdByLRU(path, isWriteLock, null);
	}

	/**
	 * check whether the given path's exists. If it doesn't exist, add it to
	 * list. If the list has been full(list.size == maxLRUNodeNumber), remove
	 * the last one. If it exists, raise this processor to the first position
	 *
	 * @param path
	 *            - measurement path in name space.
	 * @param isWriteLock
	 * @param args
	 *            - save some key-value information used for constructing a
	 *            special processor
	 * @return return processor which has the specified nsPath
	 * @throws ProcessorException
	 * @throws IOException
	 */
	public synchronized T getProcessorWithDeltaObjectIdByLRU(String path, boolean isWriteLock, Map<String, Object> args)
			throws ProcessorException, IOException, WriteProcessException {
		String nsPath;
		try {
			nsPath = mManager.getFileNameByPath(path);
		} catch (PathErrorException e) {
			throw new ProcessorException(e);
		}
		return getProcessorByLRU(nsPath, isWriteLock, args);
	}

	/**
	 * check the processor exists or not by namespacepath
	 * 
	 * @param namespacePath
	 * @return
	 */
	public synchronized boolean containNamespacePath(String namespacePath) {
		return processorMap.containsKey(namespacePath);
	}

	/**
	 * Get processor using namespacepath
	 * 
	 * @param namespacePath
	 * @param isWriteLock
	 *            true if add a write lock on return {@code T}
	 * @return
	 * @throws ProcessorException
	 * @throws IOException
	 */
	public T getProcessorByLRU(String namespacePath, boolean isWriteLock) throws ProcessorException, IOException, WriteProcessException {
		return getProcessorByLRU(namespacePath, isWriteLock, null);
	}

	/**
	 * 
	 * check whether the given namespace path exists. If it doesn't exist, add
	 * it to list. If the list has been full(list.size == maxLRUNodeNumber),
	 * remove the last one. If it exists, raise this processor to the first
	 * position
	 * 
	 * @param namespacePath
	 * @param isWriteLock
	 * @param args
	 * @return
	 * @throws ProcessorException
	 *             throw it for meeting InterruptedException while removing
	 *             unused processor
	 * @throws IOException
	 */
	public synchronized T getProcessorByLRU(String namespacePath, boolean isWriteLock, Map<String, Object> args)
			throws ProcessorException, IOException, WriteProcessException {
		LOG.debug("Try to getProcessorByLRU, {}, nsPath:{}-Thread id {}", this.getClass().getSimpleName(),
				namespacePath, Thread.currentThread().getId());
		T processor;
		if (processorMap.containsKey(namespacePath)) {
			LOG.debug("{} contains nsPath:{}", this.getClass().getSimpleName(), namespacePath);
			processor = processorMap.get(namespacePath);
			processor.lock(isWriteLock);
			initProcessor(processor, namespacePath, args);
			processorLRUList.remove(processor);
			processorLRUList.addFirst(processor);
			LOG.debug("Use {}:{}, raise it to top", processor.getClass().getSimpleName(), processor.getNameSpacePath());
		} else {
			if (processorLRUList.size() == maxLRUNodeNumber) {
				LOG.debug(
						"LRU list doesn't contain the processor and is full, remove the oldest unused processor firstly");
				try {
					removeLastUnusedProcessor();
				} catch (InterruptedException e) {
					throw new ProcessorException(
							"thread is interrupted. nsPath:" + namespacePath + " reason: " + e.getMessage());
				}
			}
			LOG.debug("Construct and init a new processor in {}:{}", this.getClass().getSimpleName(), namespacePath);
			processor = constructNewProcessor(namespacePath, args);
			initProcessor(processor, namespacePath, args);
			processorLRUList.addFirst(processor);
			processorMap.put(namespacePath, processor);
			processor.lock(isWriteLock);
			LOG.debug("{} LUR list: size is {}, content:{}", this.getClass().getSimpleName(), processorLRUList.size(),
					processorLRUList);
		}
		return processor;
	}

	/**
	 * remove the last unused processor if the LRU list is full
	 * 
	 * @throws InterruptedException
	 */
	private void removeLastUnusedProcessor() throws InterruptedException {
		while (true) {
			int index = -1;
			for (int i = processorLRUList.size() - 1; i >= 0; i--) {
				T pro = processorLRUList.get(i);
				if (pro.tryWriteLock()) {
					try {
						LOG.debug("Try write lock successfully, class:{},namespace:{}", pro.getClass().getSimpleName(),
								pro.getNameSpacePath());
						if (pro.canBeClosed()) {
							index = i;
							LOG.debug("find a removable processor, index:{}, namespace:{} break loop", index,
									pro.getNameSpacePath());
							break;
						} else {
							LOG.debug("{}:{} cannot be removed", pro.getClass().getSimpleName(),
									pro.getNameSpacePath());
						}
					} finally {
						pro.writeUnlock();
					}
				} else {
					LOG.info("Can't remove the processor {} namespace path is {}", pro.getClass().getSimpleName(),
							pro.getNameSpacePath());
				}
			}
			if (index != -1) {
				T pro = processorLRUList.get(index);
				pro.close();
				processorLRUList.remove(index);
				String proNsPath = pro.getNameSpacePath();
				processorMap.remove(proNsPath);
				LOG.debug("Remove the last processor {}:{}", pro.getClass().getSimpleName(), proNsPath);
				break;
			} else {
				LOG.debug("All processors are occupied, sleep {} millisecond", removeCheckInterval);
				Thread.sleep(removeCheckInterval);
			}
		}
	}

	/**
	 * <p>
	 * Try to close the processor of the nsPath<br>
	 * 
	 * @param nsPath
	 * @param isSync
	 *            true must close this processor, false close this processor if
	 *            possible
	 * @return
	 */
	public synchronized boolean close(String nsPath, boolean isSync) {
		if (!processorMap.containsKey(nsPath))
			return true;
		LRUProcessor pro = processorMap.get(nsPath);
		if (!isSync && !pro.tryWriteLock()) {
			return false;
		}
		try {
			do {
				if (pro.canBeClosed()) {
					pro.close();
					processorMap.remove(nsPath);
					processorLRUList.remove(pro);
					LOG.info("Close {} file:{}", pro.getClass().getSimpleName(), nsPath);
					return true;
				} else if (isSync) {
					try {
						LOG.info("Can't close {} file:{}, sleep 1000ms", pro.getClass(), nsPath);
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						throw new ErrorDebugException(e);
					}
				} else
					return false;
			} while (true);
		} finally {
			pro.writeUnlock();
		}
	}

	/**
	 * <p>
	 * close all processor still exists in memory<br>
	 */
	public synchronized void close() {
		List<String> cannotRemoveList = new ArrayList<>();

		do {
			cannotRemoveList.clear();
			for (String nsPath : processorMap.keySet()) {
				cannotRemoveList.add(nsPath);
			}
			for (String nsPath : cannotRemoveList) {
				close(nsPath, false);
			}
			if (!processorMap.isEmpty()) {
				LOG.info("{}:{} Can't be remove, waiting 1000ms", processorMap.get(cannotRemoveList.get(0)),
						cannotRemoveList);
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					throw new ErrorDebugException(e);
				}
			} else {
				processorMap.clear();
				processorLRUList.clear();
				break;
			}
		} while (true);
	}

	/**
	 * <p>
	 * construct processor using namespacepath and key-value object<br>
	 * 
	 * @param namespacePath
	 * @param args
	 * @return
	 * @throws ProcessorException
	 * @throws IOException
	 */
	protected abstract T constructNewProcessor(String namespacePath, Map<String, Object> args)
			throws ProcessorException, IOException, WriteProcessException;

	/**
	 * <p>
	 * initialize the processor with the key-value object<br>
	 * 
	 * @param processor
	 * @param namespacePath
	 * @param args
	 * @throws ProcessorException
	 */
	protected void initProcessor(T processor, String namespacePath, Map<String, Object> args)
			throws ProcessorException {
	}
}
