package cn.edu.thu.tsfiledb.service;

import java.io.File;

import org.apache.commons.io.FileUtils;

import cn.edu.thu.tsfiledb.conf.TsfileDBConfig;
import cn.edu.thu.tsfiledb.conf.TsfileDBDescriptor;

public class Monitor implements MonitorMBean{
	private TsfileDBConfig config = TsfileDBDescriptor.getInstance().getConfig();
	
	@Override
	public long getDataSize() {
		try {
			return FileUtils.sizeOfDirectory(new File(config.dataDir));
		} catch (Exception e) {
			return -1;
		}
	}

}
