package cn.edu.tsinghua.iotdb.engine.filenode;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import cn.edu.tsinghua.iotdb.utils.EnvironmentUtils;

/**
 * @author liukun
 *
 */
public class SerializeUtilTest {

	private String filePath = "serializeUtilTest";

	@Before
	public void setUp() throws Exception {
		EnvironmentUtils.closeStatMonitor();
		EnvironmentUtils.cleanDir(filePath);

	}

	@After
	public void tearDown() throws Exception {
		EnvironmentUtils.cleanDir(filePath);
	}

	@Test
	public void testHashSet() {
		Set<String> overflowset = new HashSet<String>();
		overflowset.add("set1");
		overflowset.add("set2");
		overflowset.add("set3");

		SerializeUtil<Set<String>> serializeUtil = new SerializeUtil<>();

		try {
			serializeUtil.serialize(overflowset, filePath);
		} catch (IOException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		assertEquals(true, new File(filePath).exists());

		try {
			Set<String> readSet = serializeUtil.deserialize(filePath).orElse(new HashSet<String>());
			assertEquals(overflowset, readSet);
		} catch (IOException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}

	}

	@Test
	public void testFileStore() {
		IntervalFileNode emptyIntervalFileNode = new IntervalFileNode(OverflowChangeType.NO_CHANGE, null);
		List<IntervalFileNode> newFilenodes = new ArrayList<>();
		String deviceId = "d0.s0";
		for (int i = 1; i <= 3; i++) {
			// i * 100, i * 100 + 99
			IntervalFileNode node = new IntervalFileNode(OverflowChangeType.NO_CHANGE, "bufferfiletest" + i);
			node.setStartTime(deviceId, i * 100);
			node.setEndTime(deviceId, i * 100 + 99);
			newFilenodes.add(node);
		}
		FileNodeProcessorStatus fileNodeProcessorState = FileNodeProcessorStatus.WAITING;
		Map<String, Long> lastUpdateTimeMap = new HashMap<>();
		lastUpdateTimeMap.put(deviceId, (long) 500);
		FileNodeProcessorStore fileNodeProcessorStore = new FileNodeProcessorStore(false, lastUpdateTimeMap,
				emptyIntervalFileNode, newFilenodes, fileNodeProcessorState, 0);

		SerializeUtil<FileNodeProcessorStore> serializeUtil = new SerializeUtil<>();

		try {
			serializeUtil.serialize(fileNodeProcessorStore, filePath);
		} catch (IOException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		assertEquals(true, new File(filePath).exists());
		try {
			FileNodeProcessorStore fileNodeProcessorStore2 = serializeUtil.deserialize(filePath)
					.orElse(new FileNodeProcessorStore(false, new HashMap<>(),
							new IntervalFileNode(OverflowChangeType.NO_CHANGE, null), new ArrayList<IntervalFileNode>(),
							FileNodeProcessorStatus.NONE, 0));
			assertEquals(fileNodeProcessorStore.getLastUpdateTimeMap(), fileNodeProcessorStore2.getLastUpdateTimeMap());
			assertEquals(fileNodeProcessorStore.getEmptyIntervalFileNode(),
					fileNodeProcessorStore2.getEmptyIntervalFileNode());
			assertEquals(fileNodeProcessorStore.getNewFileNodes(), fileNodeProcessorStore2.getNewFileNodes());
			assertEquals(fileNodeProcessorStore.getNumOfMergeFile(), fileNodeProcessorStore2.getNumOfMergeFile());
			assertEquals(fileNodeProcessorStore.getFileNodeProcessorStatus(),
					fileNodeProcessorStore2.getFileNodeProcessorStatus());
		} catch (IOException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

}
