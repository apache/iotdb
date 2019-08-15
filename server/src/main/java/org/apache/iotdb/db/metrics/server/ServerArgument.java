/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.metrics.server;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import com.sun.management.OperatingSystemMXBean;

public class ServerArgument {

	private static final int CPUTIME = 1000;

	private String host;
	private int port;
	private int cores;
	private long totalMemory;
	private long freeMemory;
	private long maxMemory;
	private String osName;
	private long totalPhysicalMemory;
	private long freePhysicalMemory;
	private long usedPhysicalMemory;
	private double cpuRatio;

	public ServerArgument(int port) {
		this.port = port;
		this.host = inferHostname();
		this.cores = totalCores();
		this.osName = osName();
		this.totalPhysicalMemory = totalPhysicalMemory();
		this.usedPhysicalMemory = usedPhysicalMemory();
		this.freePhysicalMemory = freePhysicalMemory();
		this.totalMemory = totalMemory();
		this.freeMemory = freeMemory();
		this.maxMemory = maxMemory();
		this.cpuRatio = getCpuRatio();
	}

	private String inferHostname() {
		InetAddress ia = null;
		try {
			ia = InetAddress.getLocalHost();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		return ia.getHostName();
	}

	private String osName() {
		return System.getProperty("os.name");
	}

	private int totalCores() {
		OperatingSystemMXBean osmxb = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
		int freeCores = osmxb.getAvailableProcessors();
		return freeCores;
	}

	private long totalMemory() {
		return Runtime.getRuntime().totalMemory() / 1024 / 1024;
	}

	private long freeMemory() {
		return Runtime.getRuntime().freeMemory() / 1024 / 1024;
	}

	private long maxMemory() {
		return Runtime.getRuntime().maxMemory() / 1024 / 1024;
	}

	private long totalPhysicalMemory() {
		OperatingSystemMXBean osmxb = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
		long totalMemorySize = osmxb.getTotalPhysicalMemorySize() / 1024 / 1024;
		return totalMemorySize;
	}

	private long usedPhysicalMemory() {
		OperatingSystemMXBean osmxb = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
		long usedMemorySize = (osmxb.getTotalPhysicalMemorySize() - osmxb.getFreePhysicalMemorySize()) / 1024 / 1024;
		return usedMemorySize;
	}

	private long freePhysicalMemory() {
		OperatingSystemMXBean osmxb = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
		long freeMemorySize = osmxb.getFreePhysicalMemorySize() / 1024 / 1024;
		return freeMemorySize;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public String getHost() {
		return host;
	}

	public int getCores() {
		return cores;
	}

	public long getTotalMemory() {
		return totalMemory;
	}

	public long getFreeMemory() {
		return freeMemory;
	}

	public long getMaxMemory() {
		return maxMemory;
	}

	public String getOsName() {
		return osName;
	}

	public long getTotalPhysicalMemory() {
		return totalPhysicalMemory;
	}

	public long getFreePhysicalMemory() {
		return freePhysicalMemory;
	}

	public long getUsedPhysicalMemory() {
		return usedPhysicalMemory;
	}

	public double getCpuRatio() {
		String osName = System.getProperty("os.name");
		cpuRatio = 0;
		if (osName.toLowerCase().startsWith("windows")) {
			cpuRatio = this.getCpuRatioForWindows();
		} else {
			cpuRatio = this.getCpuRateForLinux();
		}
		return cpuRatio;
	}

	/**
	 * cpu ratio for linux
	 */
	private double getCpuRateForLinux() {
		try {
			long[] c0 = readLinuxCpu();
			Thread.sleep(CPUTIME);
			long[] c1 = readLinuxCpu();
			if (c0 != null && c1 != null) {
				long idleCpuTime = c1[0] - c0[0];
				long totalCpuTime = c1[1] - c0[1];
				return Double.valueOf(100 * (1 - idleCpuTime / totalCpuTime)).doubleValue();
			} else {
				return 0.0;
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
			return 0.0;
		}
	}

	/**
	 * cpu ratio for windows
	 */
	private double getCpuRatioForWindows() {
		try {
			String procCmd = System.getenv("windir") + "\\system32\\wbem\\wmic.exe process get Caption,CommandLine,"
					+ "KernelModeTime,ReadOperationCount,ThreadCount,UserModeTime,WriteOperationCount";
			long[] c0 = readWinCpu(Runtime.getRuntime().exec(procCmd));
			Thread.sleep(CPUTIME);
			long[] c1 = readWinCpu(Runtime.getRuntime().exec(procCmd));
			if (c0 != null && c1 != null) {
				long idletime = c1[0] - c0[0];
				long busytime = c1[1] - c0[1];
				return Double.valueOf(100 * (busytime) / (busytime + idletime)).doubleValue();
			} else {
				return 0.0;
			}
		} catch (Exception ex) {
			ex.printStackTrace();
			return 0.0;
		}
	}

	/**
	 * read cpu info(windows)
	 */
	private long[] readWinCpu(final Process proc) {
		long[] retn = new long[2];
		try {
			proc.getOutputStream().close();
			InputStreamReader ir = new InputStreamReader(proc.getInputStream());
			LineNumberReader input = new LineNumberReader(ir);
			String line = input.readLine();
			if (line == null || line.length() < 10) {
				return null;
			}
			int capidx = line.indexOf("Caption");
			int cmdidx = line.indexOf("CommandLine");
			int rocidx = line.indexOf("ReadOperationCount");
			int umtidx = line.indexOf("UserModeTime");
			int kmtidx = line.indexOf("KernelModeTime");
			int wocidx = line.indexOf("WriteOperationCount");
			long idletime = 0;
			long kneltime = 0;
			long usertime = 0;
			while ((line = input.readLine()) != null) {
				if (line.length() < wocidx) {
					continue;
				}
				String caption = substring(line, capidx, cmdidx - 1).trim();
				String cmd = substring(line, cmdidx, kmtidx - 1).trim();
				if (cmd.indexOf("wmic.exe") >= 0) {
					continue;
				}
				if (caption.equals("System Idle Process") || caption.equals("System")) {
					idletime += Long.valueOf(substring(line, kmtidx, rocidx - 1).trim()).longValue();
					idletime += Long.valueOf(substring(line, umtidx, wocidx - 1).trim()).longValue();
					continue;
				}
				kneltime += Long.valueOf(substring(line, kmtidx, rocidx - 1).trim()).longValue();
				usertime += Long.valueOf(substring(line, umtidx, wocidx - 1).trim()).longValue();
			}
			retn[0] = idletime;
			retn[1] = kneltime + usertime;
			return retn;
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			try {
				proc.getInputStream().close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return null;
	}

	/**
	 * read cpu info(linux)
	 */
	public long[] readLinuxCpu() {
		long[] retn = new long[2];
		BufferedReader buffer = null;
		// 分别为系统启动后空闲的CPU时间和总的CPU时间
		long idleCpuTime = 0;
		long totalCpuTime = 0;
		try {
			buffer = new BufferedReader(new InputStreamReader(new FileInputStream("/proc/stat")));
			String line = null;
			while ((line = buffer.readLine()) != null) {
				if (line.startsWith("cpu")) {
					StringTokenizer tokenizer = new StringTokenizer(line);
					List<String> temp = new ArrayList<String>();
					while (tokenizer.hasMoreElements()) {
						temp.add(tokenizer.nextToken());
					}
					idleCpuTime = Long.parseLong(temp.get(4));
					for (String s : temp) {
						if (!s.equals("cpu")) {
							totalCpuTime += Long.parseLong(s);
						}
					}
					break;
				}
			}
			retn[0] = idleCpuTime;
			retn[1] = totalCpuTime;
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				buffer.close();
			} catch (Exception e2) {
				e2.printStackTrace();
			}
		}
		return retn;
	}

	private String substring(String src, int start_idx, int end_idx) {
		byte[] b = src.getBytes();
		String tgt = "";
		for (int i = start_idx; i <= end_idx; i++) {
			tgt += (char) b[i];
		}
		return tgt;
	}
}
