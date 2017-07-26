package cn.edu.thu.tsfiledb.engine.lru;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Timer;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfile.common.exception.ProcessorException;
import cn.edu.thu.tsfiledb.engine.exception.LRUManagerException;
import cn.edu.thu.tsfiledb.engine.lru.LRUProcessor;
import cn.edu.thu.tsfiledb.exception.PathErrorException;

import cn.edu.thu.tsfiledb.metadata.MManager;

/**
 * <p>
 * LRUManager manage a list of processor: {@link FileNodeProcessor}
 * processor<br>
 *
 * @author kangrong
 * @author liukun
 */
public abstract class LRUManager<T extends LRUProcessor> {
	private static final Logger LOGGER = LoggerFactory.getLogger(LRUManager.class);

	private static final long removeCheckInterval = 100;
	// The insecure variable in multiple thread
	private LinkedList<T> processorLRUList;
	private Map<String, T> processorMap;

	private int maxLRUNodeNumber;
	protected final MManager mManager;
	protected final String normalDataDir;

	protected LRUManager(int maxLRUNumber, MManager mManager, String normalDataDir) {
		this.mManager = mManager;
		this.maxLRUNodeNumber = maxLRUNumber;
		LOGGER.info("The max of LRUProcessor number of {} is {}", this.getClass().getSimpleName(), maxLRUNumber);
		processorLRUList = new LinkedList<>();
		processorMap = new HashMap<>();

		if (normalDataDir.charAt(normalDataDir.length() - 1) != File.separatorChar)
			normalDataDir += File.separatorChar;
		this.normalDataDir = normalDataDir;
		File dir = new File(normalDataDir);
		if (dir.mkdirs()) {
			LOGGER.info("{} dir home is not exists, create it", this.getClass().getSimpleName());
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
	 * check the processor exists or not by namespacepath
	 * 
	 * @param namespacePath
	 * @return
	 */
	public boolean containNamespacePath(String namespacePath) {
		synchronized (processorMap) {
			return processorMap.containsKey(namespacePath);
		}
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
	public T getProcessorWithDeltaObjectIdByLRU(String path, boolean isWriteLock) throws LRUManagerException {
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
	 * @param parameters
	 *            - save some key-value information used for constructing a
	 *            special processor
	 * @return return processor which has the specified nsPath
	 * @throws ProcessorException
	 * @throws IOException
	 */
	public T getProcessorWithDeltaObjectIdByLRU(String path, boolean isWriteLock, Map<String, Object> parameters)
			throws LRUManagerException {
		String nsPath;
		try {
			// ensure thread securely by ZJR
			nsPath = mManager.getFileNameByPath(path);
		} catch (PathErrorException e) {
			LOGGER.error("MManager get nameSpacePath error, path is {}", path);
			throw new LRUManagerException(e);
		}
		return getProcessorByLRU(nsPath, isWriteLock, parameters);
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
	public T getProcessorByLRU(String namespacePath, boolean isWriteLock) throws LRUManagerException {
		return getProcessorByLRU(namespacePath, isWriteLock, null);
	}

	/**
	 * Get the Processor from memory or construct one new processor by the
	 * nameSpacePath</br>
	 * 
	 * check whether the given namespace path exists. If it doesn't exist, add
	 * it to list. If the list has been full(list.size == maxLRUNodeNumber),
	 * remove the last one. If it exists, raise this processor to the first
	 * position
	 * 
	 * @param namespacePath
	 * @param isWriteLock
	 * @param parameters
	 * @return
	 * @throws LRUManagerException
	 */
	public T getProcessorByLRU(String namespacePath, boolean isWriteLock, Map<String, Object> parameters)
			throws LRUManagerException {

		T processor = null;
		LOGGER.debug("Try to get LRUProcessor, the nameSpacePath is {}, Thread is {}", namespacePath,
				Thread.currentThread().getName());
		// change the processorMap position and improve concurrent performance
		synchronized (processorMap) {
			LOGGER.debug("The Thread {} will get the LRUProcessor, the nameSpacePath is {}",
					Thread.currentThread().getName(), namespacePath);
			if (processorMap.containsKey(namespacePath)) {
				processor = processorMap.get(namespacePath);
				// should use the try lock
				// Notice： the lock time
				// processor.lock(isWriteLock);
				if (!processor.tryLock(isWriteLock)) {
					return null;
				}
				processorLRUList.remove(processor);
				processorLRUList.addFirst(processor);
				LOGGER.debug("The Thread {} get the LRUProcessor in memory, the nameSpacePath is {}",
						Thread.currentThread().getName(), namespacePath);
			} else {
				if (processorLRUList.size() == maxLRUNodeNumber) {
					LOGGER.warn("The LRU list is full, remove the oldest unused processor");
					// try to remove the last unused processor, if fail, return
					// null
					if (!removeLastUnusedProcessor()) {
						return null;
					}
				}
				// construct a new processor
				processor = constructNewProcessor(namespacePath);
				// must use lock and not try lock, because of this processor is
				// a new processor
				processor.lock(isWriteLock);
				processorLRUList.addFirst(processor);
				processorMap.put(namespacePath, processor);
				LOGGER.debug("The thread {} get the LRUProcessor by construction, the nameSpacePath is {}",
						Thread.currentThread().getName(), namespacePath);
			}
			return processor;
		}
	}

	private boolean removeLastUnusedProcessor() throws LRUManagerException {
		// must remove the last unused processor
		for (int i = processorLRUList.size() - 1; i >= 0; i--) {
			T processor = processorLRUList.get(i);
			if (processor.tryWriteLock()) {
				try {
					LOGGER.debug("Get the write lock for processor in memory, the nameSpacePath is {}",
							processor.getNameSpacePath());
					if (processor.canBeClosed()) {
						try {
							processor.close();
						} catch (ProcessorException e) {
							LOGGER.error("Close processor error when remove one processor, the nameSpacePath is {}",
									processor.getNameSpacePath());
							throw new LRUManagerException(e);
						}
						processorLRUList.remove(processor);
						processorMap.remove(processor.getNameSpacePath());
						LOGGER.debug(
								"Get the write lock for processor in memory, and close it, the nameSpacePath is {}",
								processor.getNameSpacePath());
						return true;
					} else {
						LOGGER.debug(
								"Get the write lock for processor in memory, but can't be closed, the nameSpacePath is {}",
								processor.getNameSpacePath());
					}
				} finally {
					processor.writeUnlock();
				}
			} else {
				LOGGER.debug("Can't get the write lock for processor in memory, the nameSpacePath is {}",
						processor.getNameSpacePath());
				// if the last update processor can't be closed, should wait to
				// remove the next
				try {
					Thread.sleep(removeCheckInterval);
				} catch (InterruptedException e) {
					LOGGER.error("Interrupted when wait to remove last unused processor");
					e.printStackTrace();
				}
			}
		}
		LOGGER.warn("Can't find the unused processor for one loop");
		return false;
	}

	/**
	 * Try to close the special processor whose nameSpacePath is nsPath Notice:
	 * this function may not close the special processor
	 * 
	 * @param nsPath
	 * @throws LRUManagerException
	 */
	private void close(String nsPath, Iterator<Entry<String, T>> processorIterator) throws LRUManagerException {
		if (processorMap.containsKey(nsPath)) {
			LRUProcessor processor = processorMap.get(nsPath);
			if (processor.tryWriteLock()) {
				try {
					if (processor.canBeClosed()) {
						try {
							LOGGER.info("Close the processor, the nameSpacePath is {}", nsPath);
							processor.close();
						} catch (ProcessorException e) {
							LOGGER.error("Close processor error when close one processor, the nameSpacePath is {}",
									nsPath);
							throw new LRUManagerException(e);
						}
						// remove from map and list
						processorIterator.remove();
						processorLRUList.remove(processor);
					} else {
						LOGGER.warn("The processor can't be closed, the nameSpacePath is {}", nsPath);
					}
				} finally {
					processor.writeUnlock();
				}
			} else {
				LOGGER.warn("Can't get the write lock the processor and close the processor, the nameSpacePath is {}",
						nsPath);
			}
		} else {
			LOGGER.warn("The processorMap does't contains the nameSpacePath {}", nsPath);
		}
	}

	/**
	 * Try to close all processors which can be closed now.
	 * 
	 * @return false - can't close all processors true - close all processors
	 * @throws LRUManagerException
	 */
	protected boolean closeAll() throws LRUManagerException {
		synchronized (processorMap) {
			Iterator<Entry<String, T>> processorIterator = processorMap.entrySet().iterator();
			while (processorIterator.hasNext()) {
				Entry<String, T> processorEntry = processorIterator.next();
				try {
					close(processorEntry.getKey(), processorIterator);
				} catch (LRUManagerException e) {
					LOGGER.error("Close processor error when close all processors, the nameSpacePath is {}",
							processorEntry.getKey());
					throw e;
				}
			}
			return processorMap.isEmpty();
		}
	}

	/**
	 * close one processor which is in the LRU list
	 * @param namespacePath
	 * @return 
	 * @throws LRUManagerException
	 */
	protected boolean closeOneProcessor(String namespacePath) throws LRUManagerException {
		synchronized (processorMap) {
			if (processorMap.containsKey(namespacePath)) {
				LRUProcessor processor = processorMap.get(namespacePath);
				try {
					processor.writeLock();
					// wait until the processor can be closed
					while (!processor.canBeClosed()) {
						try {
							TimeUnit.MILLISECONDS.sleep(100);
						} catch (InterruptedException e) {
							e.printStackTrace();
							LOGGER.warn("Wait to close the processor, the reason is {}", e.getMessage());
						}
					}
					processor.close();
					processorLRUList.remove(processor);
					processorMap.remove(namespacePath);
				} catch (ProcessorException e) {
					e.printStackTrace();
					LOGGER.error("Close processor error when close one processor, the nameSpacePath is {}",
							namespacePath);
					throw new LRUManagerException(e);
				} finally {
					processor.writeUnlock();
				}
			}
		}
		return true;
	}

	/**
	 * <p>
	 * construct processor using namespacepath and key-value object<br>
	 * 
	 * @param namespacePath
	 * @param parameters
	 * @return
	 * @throws LRUManagerException
	 */
	protected abstract T constructNewProcessor(String namespacePath) throws LRUManagerException;

	/**
	 * <p>
	 * initialize the processor with the key-value object<br>
	 * 
	 * @param processor
	 * @param namespacePath
	 * @param parameters
	 * @throws LRUManagerException
	 */
	protected abstract void initProcessor(T processor, String namespacePath, Map<String, Object> parameters)
			throws LRUManagerException;
}
