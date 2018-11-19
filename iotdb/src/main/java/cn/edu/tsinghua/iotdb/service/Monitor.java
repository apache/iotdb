package cn.edu.tsinghua.iotdb.service;

import java.io.File;
import cn.edu.tsinghua.iotdb.conf.TsFileDBConstant;
import cn.edu.tsinghua.iotdb.conf.TsfileDBConfig;
import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;
import cn.edu.tsinghua.iotdb.utils.OpenFileNumUtil;
import cn.edu.tsinghua.iotdb.exception.StartupException;
import org.apache.commons.io.FileUtils;

public class Monitor implements MonitorMBean, IService{
	private TsfileDBConfig config = TsfileDBDescriptor.getInstance().getConfig();
	public static final Monitor INSTANCE = new Monitor();
    private final String MBEAN_NAME = String.format("%s:%s=%s", TsFileDBConstant.IOTDB_PACKAGE, TsFileDBConstant.JMX_TYPE, getID().getJmxName());

	@Override
	public long getDataSizeInByte() {
		try {
			return FileUtils.sizeOfDirectory(new File(config.dataDir));
		} catch (Exception e) {
			return -1;
		}
	}

	@Override
	public int getFileNodeNum() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long getOverflowCacheSize() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long getBufferWriteCacheSize() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String getBaseDirectory() {
		try {
			File file = new File(config.dataDir);
			return file.getAbsolutePath();
		} catch (Exception e) {
			return "Unavailable";
		}
	}

	@Override
	public boolean getWriteAheadLogStatus() {
		return config.enableWal;
	}

	@Override
	public int getTotalOpenFileNum() {
		return OpenFileNumUtil.getInstance().get(OpenFileNumUtil.OpenFileNumStatistics.TOTAL_OPEN_FILE_NUM);
	}

	@Override
	public int getDataOpenFileNum() {
		return OpenFileNumUtil.getInstance().get(OpenFileNumUtil.OpenFileNumStatistics.DATA_OPEN_FILE_NUM);
	}

	@Override
	public int getDeltaOpenFileNum() {
		return OpenFileNumUtil.getInstance().get(OpenFileNumUtil.OpenFileNumStatistics.DELTA_OPEN_FILE_NUM);
	}

	@Override
	public int getOverflowOpenFileNum() {
		return OpenFileNumUtil.getInstance().get(OpenFileNumUtil.OpenFileNumStatistics.OVERFLOW_OPEN_FILE_NUM);
	}

	@Override
	public int getWalOpenFileNum() {
		return OpenFileNumUtil.getInstance().get(OpenFileNumUtil.OpenFileNumStatistics.WAL_OPEN_FILE_NUM);
	}

	@Override
	public int getMetadataOpenFileNum() {
		return OpenFileNumUtil.getInstance().get(OpenFileNumUtil.OpenFileNumStatistics.METADATA_OPEN_FILE_NUM);
	}

	@Override
	public int getDigestOpenFileNum() {
		return OpenFileNumUtil.getInstance().get(OpenFileNumUtil.OpenFileNumStatistics.DIGEST_OPEN_FILE_NUM);
	}

	@Override
	public int getSocketOpenFileNum() {
		return OpenFileNumUtil.getInstance().get(OpenFileNumUtil.OpenFileNumStatistics.SOCKET_OPEN_FILE_NUM);
	}

	@Override
	public long getMergePeriodInSecond() {
		return config.periodTimeForMerge;
	}

	@Override
	public long getClosePeriodInSecond() {
		return config.periodTimeForFlush;
	}

	@Override
	public void start() throws StartupException {
		try {
			JMXService.registerMBean(INSTANCE, MBEAN_NAME);
		} catch (Exception e) {
			String errorMessage = String.format("Failed to start %s because of %s", this.getID().getName(), e.getMessage());
			throw new StartupException(errorMessage);
		}
	}

	@Override
	public void stop() {
		JMXService.deregisterMBean(MBEAN_NAME);
	}

	@Override
	public ServiceType getID() {
		return ServiceType.MONITOR_SERVICE;
	}

}
