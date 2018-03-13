package cn.edu.tsinghua.iotdb.engine.overflow.index;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import cn.edu.tsinghua.iotdb.engine.overflow.utils.OverflowOpType;
import cn.edu.tsinghua.iotdb.engine.overflow.utils.TimePair;
import cn.edu.tsinghua.iotdb.exception.OverflowWrongParameterException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.tsfile.common.exception.UnSupportedDataTypeException;
import cn.edu.tsinghua.tsfile.common.utils.Binary;
import cn.edu.tsinghua.tsfile.common.utils.BytesUtils;
import cn.edu.tsinghua.tsfile.common.utils.ReadWriteStreamUtils;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.FilterFactory;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.filterseries.FilterSeriesType;
import cn.edu.tsinghua.tsfile.timeseries.filter.utils.LongInterval;
import cn.edu.tsinghua.tsfile.timeseries.filter.verifier.FilterVerifier;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;

/**
 * IntervalTree is a data structure implemented used Treap. </br>
 * An IntervalTree stores many TreeNodes, a TreeNode saves a time range
 * operation. </br>
 * A time range operation is explained in OverflowOpType. </br>
 * see https://en.wikipedia.org/wiki/Treap.
 *
 * @author CGF
 */
public class IntervalTree {

	private static final Logger LOG = LoggerFactory.getLogger(IntervalTree.class);

	private int nodeSize = 0; // num of tree nodes
	private int valueSize = 0; // num of bytes of value
	private long memoryUsage = 0; // total memory of the IntervalTree
	private TreeNode root = null;
	private TSDataType dataType = null; // The data type of IntervalTree
										// structure.

	public IntervalTree() {

	}

	public IntervalTree(TSDataType dataType) {
		this.dataType = dataType;
		this.memoryUsage = 0;
		if (dataType == TSDataType.TEXT) {
			this.valueSize = -1; // -1 represents var length encoding.
		}
	}

	/**
	 * TimePair must be no conflict with current IntervalTree.
	 *
	 * @param tp
	 *            TimePair to be inserted.
	 */
	private void insert(TimePair tp) {
		nodeSize++;
		calcMemoryUsage(tp);
		if (root == null) {
			root = new TreeNode(tp.s, tp.e, tp.v, tp.opType);
		} else {
			insertNoConflict(tp, root, null, false);
		}
	}

	/**
	 * Used to insert the no conflicting TreeNode.
	 *
	 * @param timePair
	 *            time pair to be inserted
	 * @param currentNode
	 *            current TreeNode
	 * @param parentNode
	 *            parent TreeNode
	 * @param left
	 *            whether the pNode is left child of paNode.
	 */
	private void insertNoConflict(TimePair timePair, TreeNode currentNode, TreeNode parentNode, boolean left) {
		if (currentNode.start > timePair.e) {
			if (currentNode.left == null) {
				currentNode.left = new TreeNode(timePair.s, timePair.e, timePair.v, timePair.opType);
			} else {
				insertNoConflict(timePair, currentNode.left, currentNode, true);
			}
			if (currentNode.fix > currentNode.left.fix)
				rightTurn(currentNode, parentNode, left);
		} else if (currentNode.end < timePair.s) {
			if (currentNode.right == null) {
				currentNode.right = new TreeNode(timePair.s, timePair.e, timePair.v, timePair.opType);
			} else {
				insertNoConflict(timePair, currentNode.right, currentNode, false);
			}
			if (currentNode.fix > currentNode.right.fix)
				leftTurn(currentNode, parentNode, left);
		}
	}

	private void leftTurn(TreeNode currentNode, TreeNode parentNode, boolean left) {
		if (parentNode == null) {
			root = currentNode.right;
		} else if (left) {
			parentNode.left = currentNode.right;
		} else {
			parentNode.right = currentNode.right;
		}
		TreeNode tmpNode = currentNode.right.left;
		currentNode.right.left = currentNode;
		currentNode.right = tmpNode;
	}

	private void rightTurn(TreeNode currentNode, TreeNode parentNode, boolean left) {
		if (parentNode == null) {
			root = currentNode.left;
		} else if (left) {
			parentNode.left = currentNode.left;
		} else {
			parentNode.right = currentNode.left;
		}
		TreeNode tmpNode = currentNode.left.right;
		currentNode.left.right = currentNode;
		currentNode.left = tmpNode;
	}

	private void delete(TreeNode currentNode, TreeNode parentNode, boolean left) {

		if (parentNode == null) {
			if (currentNode.left == null && currentNode.right == null) {
				nodeSize--;
				calcMemoryUsage(currentNode);
				root = null;
				return;
			} else if (currentNode.left == null) {
				nodeSize--;
				calcMemoryUsage(currentNode);
				root = currentNode.right;
				return;
			} else if (currentNode.right == null) {
				nodeSize--;
				calcMemoryUsage(currentNode);
				root = currentNode.left;
				return;
			}
		}

		if (left) {
			if (currentNode.left == null) {
				nodeSize--;
				calcMemoryUsage(currentNode);
				parentNode.left = currentNode.right;
			} else if (currentNode.right == null) {
				nodeSize--;
				calcMemoryUsage(currentNode);
				parentNode.left = currentNode.left;
			} else {
				if (currentNode.left.fix < currentNode.right.fix) {
					TreeNode tmpNode = currentNode.left;
					rightTurn(currentNode, parentNode, left);
					delete(currentNode, tmpNode, false);
				} else {
					TreeNode tmpNode = currentNode.right;
					leftTurn(currentNode, parentNode, left);
					delete(currentNode, tmpNode, true);
				}
			}
		} else {
			if (currentNode.left == null) {
				nodeSize--;
				calcMemoryUsage(currentNode);
				parentNode.right = currentNode.right;
			} else if (currentNode.right == null) {
				nodeSize--;
				calcMemoryUsage(currentNode);
				parentNode.right = currentNode.left;
			} else {
				if (currentNode.left.fix < currentNode.right.fix) {
					TreeNode tmpNode = currentNode.left;
					rightTurn(currentNode, parentNode, left);
					delete(currentNode, tmpNode, false);
				} else {
					TreeNode tmpNode = currentNode.right;
					leftTurn(currentNode, parentNode, left);
					delete(currentNode, tmpNode, true);
				}
			}
		}
	}

	/**
	 * Update operation api.
	 *
	 * @param tp
	 *            TimePair to be updated
	 */
	public void update(TimePair tp) {

		if (this.dataType == null) {
			if (tp.opType != OverflowOpType.DELETE) {
				this.valueSize = tp.v.length;
			}
			this.dataType = tp.dataType;
		} else if (tp.dataType != this.dataType) {
			throw new OverflowWrongParameterException(String.format("IntervalTree wrong time pair parameters, " +
					"update time pair data type is %s, exist time pair data type is %s.", tp.dataType, this.dataType));
		}

		// When an UPDATE operation covers root(INSERT) node,
		// we delete root(INSERT) node and stores it in insertList temporary.
		List<TimePair> insertList = new ArrayList<>();
		while (root != null) {
			if (update(tp, root, null, false, insertList)) {
				break;
			}
		}

		if (root == null) {
			insert(tp);
		}

		for (TimePair t : insertList) {
			while (root != null) {
				if (update(t, root, null, false, null)) {
					break;
				}
			}
		}
	}

	/**
	 * IntervalTree update operation.
	 *
	 * @param tp
	 *            TimePair to be updated
	 * @param currentNode
	 *            current TreeNode
	 * @param parentNode
	 *            parent node of pNode
	 * @param left
	 *            whether pNode is left child of paNode
	 * @return whether update process is over
	 */
	private boolean update(TimePair tp, TreeNode currentNode, TreeNode parentNode, boolean left, List<TimePair> insertList) {
		if (currentNode == null) {
			insert(tp);
			return true;
		}

		CrossRelation relation = IntervalRelation.getRelation(tp, new TimePair(currentNode.start, currentNode.end));
		switch (relation) {
		case LCOVERSR: // tp covers currentNode
			if (currentNode.opType == OverflowOpType.INSERT && tp.opType == OverflowOpType.UPDATE) {
				insertList.add(new TimePair(currentNode.start, currentNode.end, tp.v, OverflowOpType.INSERT));
			}
			if (currentNode.opType == OverflowOpType.DELETE && tp.opType == OverflowOpType.UPDATE) {
				tp.s = currentNode.end + 1;
			} else {
				delete(currentNode, parentNode, left);
			}
			return false;
		case RCOVERSL: // currentNode covers tp
			if (tp.s == currentNode.start && currentNode.end == tp.e) {
				if (currentNode.opType == OverflowOpType.INSERT && tp.opType == OverflowOpType.UPDATE) {
					currentNode.value = tp.v;
				} else if (currentNode.opType == OverflowOpType.DELETE && tp.opType == OverflowOpType.UPDATE) {
					return true;
				} else {
					currentNode.opType = tp.opType;
					currentNode.value = tp.v;
				}
			} else if (tp.s == currentNode.start) {
				if (currentNode.opType == OverflowOpType.DELETE && tp.opType == OverflowOpType.UPDATE) {
					return true;
				} else {
					currentNode.start = tp.e + 1;
					insert(tp);
				}
			} else if (tp.e == currentNode.end) {
				if (currentNode.opType == OverflowOpType.DELETE && tp.opType == OverflowOpType.UPDATE) {
					return true;
				} else {
					currentNode.end = tp.s - 1;
					insert(tp);
				}
			} else { // (currentNode tp currentNode)
				if (currentNode.opType == OverflowOpType.DELETE && tp.opType == OverflowOpType.UPDATE) {
					return true;
				} else {
					long tmp = currentNode.end;
					currentNode.end = tp.s - 1;
					insert(tp);
					insert(new TimePair(tp.e + 1, tmp, currentNode.value, currentNode.opType));
				}
			}
			return true;
		case LFIRSTCROSS: // tp first cross | No need to modify currentNode
							// value.
			currentNode.start = tp.e + 1;
			return update(tp, currentNode.left, currentNode, true, insertList);
		case RFIRSTCROSS: // currentNode first cross | No need to modify
							// currentNode value.
			if (currentNode.opType == OverflowOpType.DELETE && tp.opType == OverflowOpType.UPDATE) {
				return update(new TimePair(currentNode.end + 1, tp.e, tp.v, tp.opType), currentNode.right, currentNode,
						false, insertList);
			} else {
				currentNode.end = tp.s - 1;
				return update(tp, currentNode.right, currentNode, false, insertList);
			}
		case LFIRST: // tp first
			return update(tp, currentNode.left, currentNode, true, insertList);
		case RFIRST: // currentNode first
			return update(tp, currentNode.right, currentNode, false, insertList);
		default:
			return true;
		}
	}

	/**
	 * Put the IntervalTree mid order serialization into OutputStream.
	 *
	 * @param out
	 *            OutputStream
	 */
	public void midOrderSerialize(OutputStream out) throws IOException {
		midOrderSerialize(root, out);
		out.close();
		root = null;
	}

	/**
	 * IntervalTree mid order serialization.
	 * <p>
	 * Notice that "String" data type adopts unsigned var int encoding.
	 *
	 * @param pNode
	 *            - root node of IntervalTree
	 * @param out
	 *            - output file stream
	 */
	private void midOrderSerialize(TreeNode pNode, OutputStream out) throws IOException {
		if (pNode == null)
			return;
		midOrderSerialize(pNode.left, out);
		if (pNode.opType.equals(OverflowOpType.DELETE)) {
			out.write(BytesUtils.longToBytes(-pNode.start));
			out.write(BytesUtils.longToBytes(-pNode.end));
		} else if (pNode.opType.equals(OverflowOpType.UPDATE)) {
			out.write(BytesUtils.longToBytes(pNode.start));
			out.write(BytesUtils.longToBytes(pNode.end));
			if (dataType == TSDataType.TEXT || valueSize == -1) {
				ReadWriteStreamUtils.writeUnsignedVarInt(pNode.value.length, out);
				out.write(pNode.value);
			} else {
				out.write(pNode.value);
			}
		} else {
			out.write(BytesUtils.longToBytes(pNode.start));
			out.write(BytesUtils.longToBytes(-pNode.end));
			if (dataType == TSDataType.TEXT || valueSize == -1) {
				ReadWriteStreamUtils.writeUnsignedVarInt(pNode.value.length, out);
				out.write(pNode.value);
			} else {
				out.write(pNode.value);
			}

		}
		midOrderSerialize(pNode.right, out);
	}

	/**
	 * given the query TimePair tp, return all covered time pair with tp.
	 * <p>
	 * for concurrency safety, the queryAns variable must be defined in the
	 * query method.
	 *
	 * @param tp
	 *            - TimePair to query
	 * @return - all new TimePairs accord with the query tp.
	 */
	public List<TimePair> query(TimePair tp) {
		List<TimePair> queryAns = new ArrayList<>();
		query(tp, root, queryAns);
		return queryAns;
	}

	private void query(TimePair tp, TreeNode pNode, List<TimePair> queryAns) {
		if (pNode == null)
			return;
		CrossRelation relation = IntervalRelation.getRelation(tp, new TimePair(pNode.start, pNode.end));
		switch (relation) {
		case LCOVERSR: // tp covers currentNode
			if (tp.s == pNode.start && pNode.end == tp.e) {
				queryAns.add(new TimePair(tp.s, tp.e, pNode.value, pNode.opType));
			} else if (tp.s == pNode.start) {
				query(new TimePair(pNode.end + 1, tp.e), pNode.right, queryAns);
				queryAns.add(new TimePair(pNode.start, pNode.end, pNode.value, pNode.opType));
			} else if (tp.e == pNode.end) {
				query(new TimePair(tp.s, pNode.end - 1), pNode.left, queryAns);
				queryAns.add(new TimePair(pNode.start, pNode.end, pNode.value, pNode.opType));
			} else {
				query(new TimePair(tp.s, pNode.start - 1), pNode.left, queryAns);
				queryAns.add(new TimePair(pNode.start, pNode.end, pNode.value, pNode.opType));
				query(new TimePair(pNode.end + 1, tp.e), pNode.right, queryAns);
			}
			return;
		case RCOVERSL: // currentNode covers tp
			queryAns.add(new TimePair(tp.s, tp.e, pNode.value, pNode.opType));
			return;
		case LFIRSTCROSS: // tp first cross
			query(new TimePair(tp.s, pNode.start - 1), pNode.left, queryAns);
			queryAns.add(new TimePair(pNode.start, tp.e, pNode.value, pNode.opType));
			return;
		case RFIRSTCROSS: // currentNode first cross
			queryAns.add(new TimePair(tp.s, pNode.end, pNode.value, pNode.opType));
			query(new TimePair(pNode.end + 1, tp.e), pNode.right, queryAns);
			return;
		case LFIRST: // tp first
			query(tp, pNode.left, queryAns);
			break;
		case RFIRST: // currentNode first
			query(tp, pNode.right, queryAns);
			break;
		}
	}

	/**
	 * Given time filter and value filter, return the answer
	 * List<DynamicOneColumn> in IntervalTree.</br>
	 * Could not use member variable. </br>
	 * Must notice thread safety! </br>
	 * Note that value filter is not given, apply value filter in this method
	 * would cause errors.</br>
	 * May make wrong intervals in old data valid in
	 * IntervalTreeOperation.queryFileBlock method.
	 *
	 * @param timeFilter
	 *            filter for time
	 * @param dataType
	 *            TSDataType
	 * @return DynamicOneColumnData
	 */
	public DynamicOneColumnData dynamicQuery(SingleSeriesFilterExpression timeFilter, TSDataType dataType) {

		DynamicOneColumnData crudResult = new DynamicOneColumnData(dataType, true);

		if (timeFilter == null) {
			timeFilter = FilterFactory
					.gtEq(FilterFactory.longFilterSeries("NULL", "NULL", FilterSeriesType.TIME_FILTER), 0L, true);
		}

		LongInterval val = (LongInterval) FilterVerifier.create(TSDataType.INT64).getInterval(timeFilter);
		dynamicQuery(crudResult, root, val, dataType);

		return crudResult;
	}

	/**
	 * Implemented for above dynamicQuery method.
	 *
	 * @param crudResult
	 *            DynamicOneColumnData
	 * @param pNode
	 *            query root TreeNode
	 * @param val
	 *            LongInterval
	 * @param dataType
	 *            TSDataType
	 */
	private void dynamicQuery(DynamicOneColumnData crudResult, TreeNode pNode, LongInterval val, TSDataType dataType) {
		if (pNode == null)
			return;

		// to examine whether left child node has answer
		if (val.count != 0 && val.v[0] < pNode.start) {
			dynamicQuery(crudResult, pNode.left, val, dataType);
		}

		for (int i = 0; i < val.count; i += 2) {
			CrossRelation relation = IntervalRelation.getRelation(new TimePair(val.v[i], val.v[i + 1]),
					new TimePair(pNode.start, pNode.end));
			switch (relation) {
			case LFIRST:
				continue;
			case RFIRST:
				continue;
			case LFIRSTCROSS:
				putTreeNodeValue(crudResult, pNode.opType, pNode.start, val.v[i + 1], dataType, pNode.value);
				break;
			case RFIRSTCROSS:
				putTreeNodeValue(crudResult, pNode.opType, val.v[i], pNode.end, dataType, pNode.value);
				break;
			case LCOVERSR:
				putTreeNodeValue(crudResult, pNode.opType, pNode.start, pNode.end, dataType, pNode.value);
				break;
			case RCOVERSL:
				putTreeNodeValue(crudResult, pNode.opType, val.v[i], val.v[i + 1], dataType, pNode.value);
				break;
			default:
				LOG.error("un defined CrossRelation, left time pair is [{},{}], right time pair is [{}, {}], " +
								"relation type is {}.",
						val.v[i], val.v[i + 1], pNode.start, pNode.end, relation);
			}
		}

		// to examine whether right child node has answer
		if (val.count != 0 && val.v[val.count - 1] > pNode.end) { // left node
																	// has
																	// answer
			dynamicQuery(crudResult, pNode.right, val, dataType);
		}
	}

	/**
	 * Put TreeNode information into DynamicColumnData crudResult.
	 *
	 * @param crudResult
	 *            DynamicOneColumnData stores operations
	 * @param opType
	 *            OverflowOpType
	 * @param s
	 *            start time
	 * @param e
	 *            end time
	 * @param dataType
	 *            TSDataType
	 * @param value
	 *            byte value
	 */
	private void putTreeNodeValue(DynamicOneColumnData crudResult, OverflowOpType opType, long s, long e,
								  TSDataType dataType, byte[] value) {
		switch (dataType) {
			case INT32:
				switch (opType) {
					case INSERT:
						putTimePair(crudResult, s, -e);
						crudResult.putInt(BytesUtils.bytesToInt(value));
						break;
					case DELETE:
						putTimePair(crudResult, -s, -e);
						crudResult.putInt(0);
						break;
					case UPDATE:
						putTimePair(crudResult, s, e);
						crudResult.putInt(BytesUtils.bytesToInt(value));
						break;
				}
				break;
			case INT64:
				switch (opType) {
					case INSERT:
						putTimePair(crudResult, s, -e);
						crudResult.putLong(BytesUtils.bytesToLong(value));
						break;
					case DELETE:
						putTimePair(crudResult, -s, -e);
						crudResult.putLong(0);
						break;
					case UPDATE:
						putTimePair(crudResult, s, e);
						crudResult.putLong(BytesUtils.bytesToLong(value));
				}
				break;
			case FLOAT:
				switch (opType) {
					case INSERT:
						putTimePair(crudResult, s, -e);
						crudResult.putFloat(BytesUtils.bytesToFloat(value));
						break;
					case DELETE:
						putTimePair(crudResult, -s, -e);
						crudResult.putFloat(0);
						break;
					case UPDATE:
						putTimePair(crudResult, s, e);
						crudResult.putFloat(BytesUtils.bytesToFloat(value));
						break;
				}
				break;
			case DOUBLE:
				switch (opType) {
					case INSERT:
						putTimePair(crudResult, s, -e);
						crudResult.putDouble(BytesUtils.bytesToDouble(value));
						break;
					case DELETE:
						putTimePair(crudResult, -s, -e);
						crudResult.putDouble(0);
						break;
					case UPDATE:
						putTimePair(crudResult, s, e);
						crudResult.putDouble(BytesUtils.bytesToDouble(value));
						break;
				}
				break;
			case BOOLEAN:
				switch (opType) {
					case INSERT:
						putTimePair(crudResult, s, -e);
						crudResult.putBoolean(BytesUtils.bytesToBool(value));
						break;
					case DELETE:
						putTimePair(crudResult, -s, -e);
						crudResult.putBoolean(false);;
						break;
					case UPDATE:
						putTimePair(crudResult, s, e);
						crudResult.putBoolean(BytesUtils.bytesToBool(value));
						break;
				}
				break;
			case TEXT:
				switch (opType) {
					case INSERT:
						putTimePair(crudResult, s, -e);
						crudResult.putBinary(Binary.valueOf(BytesUtils.bytesToString(value)));
						break;
					case DELETE:
						putTimePair(crudResult, -s, -e);
						crudResult.putBinary(Binary.valueOf(""));
						break;
					case UPDATE:
						putTimePair(crudResult, s, e);
						crudResult.putBinary(Binary.valueOf(BytesUtils.bytesToString(value)));
						break;
				}
				break;
			default:
				throw new UnSupportedDataTypeException("Unsupported tsfile data type.");
		}
	}

	private void calcMemoryUsage(TimePair tp) {
		if (tp.opType == OverflowOpType.DELETE) {
			memoryUsage += 16;
		} else {
			memoryUsage = memoryUsage + 16 + tp.v.length;
		}

	}

	private void calcMemoryUsage(TreeNode node) {
		if (node.opType == OverflowOpType.DELETE) {
			memoryUsage -= 16;
		} else {
			memoryUsage = memoryUsage - 16 - node.value.length;
		}

	}

	public int getNodeSize() {
		return nodeSize;
	}

	public long getTotalMemory() {
		return this.memoryUsage;
	}

	public int getValueSize() {
		return this.valueSize;
	}

	public boolean isEmpty() {
		return root == null;
	}

	private void putTimePair(DynamicOneColumnData data, long s, long e) {
		data.putTime(s);
		data.putTime(e);
	}
}
