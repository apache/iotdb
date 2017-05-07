package cn.edu.thu.tsfiledb.engine.filenode;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import cn.edu.thu.tsfiledb.engine.filenode.FileNodeProcessorState;
import cn.edu.thu.tsfiledb.engine.filenode.FileNodeProcessorStore;
import cn.edu.thu.tsfiledb.engine.filenode.IntervalFileNode;
import cn.edu.thu.tsfiledb.engine.filenode.OverflowChangeType;
import cn.edu.thu.tsfiledb.engine.filenode.SerializeUtil;
import cn.edu.thu.tsfiledb.engine.overflow.io.EngineTestHelper;

public class SerializeUtilTest {

	private String filePath = "serializeUtilTest";

	@Before
	public void setUp() throws Exception {
		EngineTestHelper.delete(filePath);

	}

	@After
	public void tearDown() throws Exception {
		EngineTestHelper.delete(filePath);
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
		IntervalFileNode emptyIntervalFileNode = new IntervalFileNode(0, OverflowChangeType.NO_CHANGE, null, null);
		List<IntervalFileNode> newFilenodes = new ArrayList<>();
		for (int i = 1; i <= 3; i++) {
			IntervalFileNode node = new IntervalFileNode(i * 100, i * 100 + 99, OverflowChangeType.NO_CHANGE,
					"bufferfiletest" + i, null);
			newFilenodes.add(node);
		}
		FileNodeProcessorState fileNodeProcessorState = FileNodeProcessorState.WAITING;

		FileNodeProcessorStore fileNodeProcessorStore = new FileNodeProcessorStore(500, emptyIntervalFileNode,
				newFilenodes, fileNodeProcessorState, 0);

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
					.orElse(new FileNodeProcessorStore(-1,
							new IntervalFileNode(0, OverflowChangeType.NO_CHANGE, null, null), new ArrayList<>(),
							FileNodeProcessorState.NONE, 0));
			assertEquals(fileNodeProcessorStore.getLastUpdateTime(), fileNodeProcessorStore2.getLastUpdateTime());
			assertEquals(fileNodeProcessorStore.getEmptyIntervalFileNode(), fileNodeProcessorStore2.getEmptyIntervalFileNode());
			assertEquals(fileNodeProcessorStore.getNewFileNodes(), fileNodeProcessorStore2.getNewFileNodes());
			assertEquals(fileNodeProcessorStore.getNumOfMergeFile(), fileNodeProcessorStore2.getNumOfMergeFile());
			assertEquals(fileNodeProcessorStore.getFileNodeProcessorState(), fileNodeProcessorStore2.getFileNodeProcessorState());
		} catch (IOException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

}
