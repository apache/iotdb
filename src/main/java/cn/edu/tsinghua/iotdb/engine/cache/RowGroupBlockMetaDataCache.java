package cn.edu.tsinghua.iotdb.engine.cache;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.tsfile.file.metadata.TsFileMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.TsRowGroupBlockMetaData;

/**
 * This class is used to cache <code>RowGroupBlockMetaDataCache</code> of tsfile
 * in IoTDB.
 * 
 * @author liukun
 *
 */
public class RowGroupBlockMetaDataCache {

	private static Logger LOGGER = LoggerFactory.getLogger(RowGroupBlockMetaDataCache.class);
	private static final int cacheSize = 100;
	/** key: the file path + DeltaObjectId */
	private LinkedHashMap<String, TsRowGroupBlockMetaData> LRUCache;
	private AtomicLong cacheHintNum = new AtomicLong();
	private AtomicLong cacheRequestNum = new AtomicLong();

	/**
	 * This class is a map used to cache the <code>RowGroupBlockMetaData</code>.
	 * The caching strategy is LRU.
	 * 
	 * @author liukun
	 *
	 */
	private class LRULinkedHashMap extends LinkedHashMap<String, TsRowGroupBlockMetaData> {

		private static final long serialVersionUID = 1290160928914532649L;
		private static final float loadFactor = 0.75f;
		private int maxCapacity;

		public LRULinkedHashMap(int maxCapacity, boolean isLRU) {
			super(maxCapacity, loadFactor, isLRU);
			this.maxCapacity = maxCapacity;
		}

		@Override
		protected boolean removeEldestEntry(Map.Entry<String, TsRowGroupBlockMetaData> eldest) {
			return size() > maxCapacity;
		}
	}

	/*
	 * the default LRU cache size is 100. The singleton pattern.
	 */
	private static class RowGroupBlockMetaDataCacheSingleton {
		private static final RowGroupBlockMetaDataCache INSTANCE = new RowGroupBlockMetaDataCache(cacheSize);
	}

	public static RowGroupBlockMetaDataCache getInstance() {
		return RowGroupBlockMetaDataCacheSingleton.INSTANCE;
	}

	private RowGroupBlockMetaDataCache(int cacheSize) {
		LRUCache = new LRULinkedHashMap(cacheSize, true);
	}

	public TsRowGroupBlockMetaData get(String filePath, String deltaObjectId, TsFileMetaData fileMetaData) {
		/** The key(the tsfile path and deltaObjectId) for the LRUCahe */
		String jointPath = filePath + deltaObjectId;
		jointPath = jointPath.intern();
		synchronized (LRUCache) {
			cacheRequestNum.incrementAndGet();
			if (LRUCache.containsKey(jointPath)) {
				cacheHintNum.incrementAndGet();
				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug(
							"Cache hint: the number of requests for cache is {}, the number of hints for cache is {}",
							cacheRequestNum.get(), cacheHintNum.get());
				}
				return LRUCache.get(jointPath);
			}
		}
		synchronized (jointPath) {
			synchronized (LRUCache) {
				if (LRUCache.containsKey(jointPath)) {
					return LRUCache.get(jointPath);
				}
			}
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("Cache didn't hint: the number of requests for cache is {}", cacheRequestNum.get());
			}
			TsRowGroupBlockMetaData blockMetaData = TsFileMetadataUtils.getTsRowGroupBlockMetaData(filePath,
					deltaObjectId, fileMetaData);
			synchronized (LRUCache) {
				LRUCache.put(jointPath, blockMetaData);
				return LRUCache.get(jointPath);
			}
		}
	}

	public void clear() {
		synchronized (LRUCache) {
			LRUCache.clear();
		}
	}
}
