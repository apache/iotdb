package org.apache.iotdb.db.engine.measurementorderoptimizer;

import org.apache.iotdb.db.engine.divergentdesign.Replica;
import org.apache.iotdb.db.engine.measurementorderoptimizer.costmodel.CostModel;
import org.apache.iotdb.db.query.workloadmanager.Workload;
import org.apache.iotdb.db.query.workloadmanager.WorkloadManager;
import org.apache.iotdb.db.query.workloadmanager.queryrecord.GroupByQueryRecord;
import org.apache.iotdb.db.query.workloadmanager.queryrecord.QueryRecord;
import org.apache.iotdb.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class MultiReplicaOrderOptimizer {
	private int replicaNum = 3;
	private int maxIter = 800000;
	private float breakPoint = 1e-2f;
	private static final Logger LOGGER = LoggerFactory.getLogger(MultiReplicaOrderOptimizer.class);
	private String deviceID;
	private Replica[] replicas;
	private List<String> measurementOrder;
	public List<QueryRecord> records;
	private List<Long> chunkSize;
	private final float SA_INIT_TEMPERATURE = 2.0f;
	private final float COOLING_RATE = 0.99f;
	private List<Double> costList = new LinkedList<>();
	private static long CHUNK_SIZE_STEP_NUM = 70000l;
	private final float CHUNK_SIZE_LOWER_BOUND = 0.8f;
	private final float CHUNK_SIZE_UPPER_BOUND = 2.0f;
	private final int GA_GENERATION_NUM = 50;
	private final float GA_CROSS_RATE = 0.15f;
	private final float GA_MUTATE_RATE = 0.10f;
	private final float BALANCE_FACTOR = 0.30f;
	private int GALoop = 0;

	public MultiReplicaOrderOptimizer(String deviceID) {
		this.deviceID = deviceID;
		measurementOrder = new ArrayList<>(MeasurementOrderOptimizer.
						getInstance().getMeasurementsOrder(deviceID));
		replicas = new Replica[replicaNum];
		for (int i = 0; i < replicaNum; ++i) {
			replicas[i] = new Replica(deviceID, measurementOrder,
							MeasurementOrderOptimizer.getInstance().getAverageChunkSize(deviceID));
		}
		records = new ArrayList<>(WorkloadManager.getInstance().getRecord(deviceID));
		chunkSize = new ArrayList<>(MeasurementOrderOptimizer.getInstance().getChunkSize(deviceID));
	}

	public MultiReplicaOrderOptimizer(String deviceID, int replicaNum) {
		this.deviceID = deviceID;
		measurementOrder = new ArrayList<>(MeasurementOrderOptimizer.
						getInstance().getMeasurementsOrder(deviceID));
		this.replicaNum = replicaNum;
		replicas = new Replica[replicaNum];
		for (int i = 0; i < replicaNum; ++i) {
			replicas[i] = new Replica(deviceID, measurementOrder,
							MeasurementOrderOptimizer.getInstance().getAverageChunkSize(deviceID));
		}
		records = new ArrayList<>(WorkloadManager.getInstance().getRecord(deviceID));
		chunkSize = new ArrayList<>(MeasurementOrderOptimizer.getInstance().getChunkSize(deviceID));
	}

	public void setMaxIter(int iter) {
		maxIter = iter;
	}

	public Pair<List<Double>, List<Long>> optimizeBySA() {
		double curCost = getCostAndWorkloadPartitionForCurReplicas(records, replicas).left;
		LOGGER.info("Ori cost: " + curCost);
		Pair<Long, Long> chunkBound = getChunkSizeBound(records);
		long chunkLowerBound = chunkBound.left;
		long chunkUpperBound = chunkBound.right;
		float temperature = SA_INIT_TEMPERATURE;
		long optimizeStartTime = System.currentTimeMillis();
		Random r = new Random();
		Workload[] workloadPartition = null;
		int k = 0;
		List<Long> timeList = new ArrayList<>();
		for (; k < maxIter && System.currentTimeMillis() - optimizeStartTime < 60l * 60l * 1000l; ++k) {
			temperature = temperature * COOLING_RATE;
			int selectedReplica = r.nextInt(replicaNum);
			int swapLeft = r.nextInt(measurementOrder.size());
			int swapRight = r.nextInt(measurementOrder.size());
			while (swapLeft == swapRight) {
				swapLeft = r.nextInt(measurementOrder.size());
				swapRight = r.nextInt(measurementOrder.size());
			}
			replicas[selectedReplica].swapMeasurementPos(swapLeft, swapRight);
			Pair<Float, Workload[]> costAndWorkloadPartition = getCostAndWorkloadPartitionForCurReplicas(records, replicas);
			double newCost = costAndWorkloadPartition.left;
			workloadPartition = costAndWorkloadPartition.right;
			float probability = r.nextFloat();
			probability = probability < 0 ? -probability : probability;
			probability %= 1.0f;
			if (newCost < curCost ||
							Math.exp((curCost - newCost) / temperature) > probability) {
				curCost = newCost;
			} else {
				replicas[selectedReplica].swapMeasurementPos(swapLeft, swapRight);
			}
			costList.add(curCost);
			timeList.add(System.currentTimeMillis() - optimizeStartTime);
			if (k % 500 == 0) {
				LOGGER.info(String.format("Epoch %d: curCost %.3f", k, curCost));
			}
		}
		LOGGER.info("Final cost: " + curCost);
		LOGGER.info("Loop count: " + k);
		return new Pair<>(costList, timeList);
	}

	public Pair<Replica[], Workload[]> optimizeBySAWithChunkSizeAdjustment() {
		double curCost = getCostAndWorkloadPartitionForCurReplicas(records, replicas).left;
		LOGGER.info("Ori cost: " + curCost);
		Pair<Long, Long> chunkBound = getChunkSizeBound(records);
		long chunkLowerBound = chunkBound.left;
		long chunkUpperBound = chunkBound.right;
		float temperature = SA_INIT_TEMPERATURE;
		long optimizeStartTime = System.currentTimeMillis();
		Random r = new Random();
		Workload[] workloadPartition = null;
		int k = 0;
		CostRecorder costRecorder = new CostRecorder();
		long startTime = System.currentTimeMillis();
		costRecorder.addCost(curCost, startTime);
		for(int i = 0; i < replicas.length; ++i) {
			Collections.shuffle(replicas[i].measurements);
		}
		for (; k < maxIter && System.currentTimeMillis() - optimizeStartTime < 40l * 60l * 1000l; ++k) {
			temperature = temperature * COOLING_RATE;
			int selectedReplica = r.nextInt(replicaNum);
			int operation = r.nextInt(2);
			if ((operation == 0 && measurementOrder.size() > 1) || k > 10000) {
				// Swap chunk order
				int swapLeft = r.nextInt(measurementOrder.size());
				int swapRight = r.nextInt(measurementOrder.size());
				while (swapLeft == swapRight) {
					swapLeft = r.nextInt(measurementOrder.size());
					swapRight = r.nextInt(measurementOrder.size());
				}
				replicas[selectedReplica].swapMeasurementPos(swapLeft, swapRight);
				Pair<Float, Workload[]> costAndWorkloadPartition = getCostAndWorkloadPartitionForCurReplicas(records, replicas);
				double newCost = costAndWorkloadPartition.left;
				workloadPartition = costAndWorkloadPartition.right;
				float probability = r.nextFloat();
				probability = probability < 0 ? -probability : probability;
				probability %= 1.0f;
				if (newCost < curCost ||
								Math.exp((curCost - newCost) / temperature) > probability) {
					curCost = newCost;
				} else {
					replicas[selectedReplica].swapMeasurementPos(swapLeft, swapRight);
				}
			} else {
				// Change chunk size
				long newChunkSize = Math.abs(r.nextLong());
				newChunkSize = newChunkSize % (chunkUpperBound - chunkLowerBound) + chunkLowerBound;
				long curChunkSize = replicas[selectedReplica].getAverageChunkSize();
				replicas[selectedReplica].setAverageChunkSize(newChunkSize);
				Pair<Float, Workload[]> costAndWorkloadPartition = getCostAndWorkloadPartitionForCurReplicas(records, replicas);
				double newCost = costAndWorkloadPartition.left;
				workloadPartition = costAndWorkloadPartition.right;
				float probability = r.nextFloat();
				probability = probability < 0 ? -probability : probability;
				probability %= 1.0f;
				if (newCost < curCost ||
								Math.exp((curCost - newCost) / temperature) > probability) {
					curCost = newCost;
				} else {
					replicas[selectedReplica].setAverageChunkSize(curChunkSize);
				}
			}
			if (!costRecorder.addCost(curCost, System.currentTimeMillis() - startTime)) {
				LOGGER.info("Break by cost recorder");
				break;
			}
			if (k % 10 == 0) {
				LOGGER.info(String.format("Epoch %d: curCost %.3f", k, curCost));
			}
		}
		getCostAndWorkloadPartitionForCurReplicas(records, replicas);
		LOGGER.info("Final cost: " + curCost);
		LOGGER.info("Loop count: " + k);
		return new Pair<>(replicas, getCostAndWorkloadPartitionForCurReplicas(records, replicas).right);
	}

	public Pair<List<Double>, List<Long>> optimizeBySAWithChunkSizeAdjustmentAndCostRecord() {
//		List<QueryRecord> tmpList = new ArrayList<>();
//		for(int i = 0; i < 1; ++i) {
//			tmpList.add(records.get(i));
//		}
//		records = tmpList;
		double curCost = getCostAndWorkloadPartitionForCurReplicas(records, replicas).left;
		LOGGER.info("Ori cost: " + curCost);
		Pair<Long, Long> chunkBound = getChunkSizeBound(records);
		long chunkLowerBound = chunkBound.left;
		long chunkUpperBound = chunkBound.right;
		float temperature = SA_INIT_TEMPERATURE;
		long optimizeStartTime = System.currentTimeMillis();
		Random r = new Random();
		Workload[] workloadPartition = getCostAndWorkloadPartitionForCurReplicas(records, replicas).right;
		int k = 0;
		CostRecorder costRecorder = new CostRecorder();
		long startTime = System.currentTimeMillis();
		costRecorder.addCost(curCost, 0l);
		boolean SCOA = false;
		int selectedReplica = r.nextInt(replicaNum);
		for (; k < maxIter && System.currentTimeMillis() - optimizeStartTime < 15l * 60l * 1000l; ++k) {
			temperature = temperature * COOLING_RATE;
			int operation = r.nextInt(2);
			if (!SCOA && System.currentTimeMillis() - optimizeStartTime > 3l*60l*1000l) {
				SCOA = true;
			}
			if (SCOA) {
				float[] replicaCost = new float[replicaNum];
				for(int i = 0; i < replicaNum; ++i) replicaCost[i] = CostModel.approximateAggregationQueryCostWithTimeRange(workloadPartition[i].getRecords(), replicas[i].getMeasurements(), replicas[i].getAverageChunkSize());
				for(int i = 0; i < replicaNum; ++i) {
					int swapLeft = r.nextInt(measurementOrder.size());
					int swapRight = r.nextInt(measurementOrder.size());
					while (swapLeft == swapRight) {
						swapLeft = r.nextInt(measurementOrder.size());
						swapRight = r.nextInt(measurementOrder.size());
					}
					replicas[i].swapMeasurementPos(swapLeft, swapRight);
					float newCost = CostModel.approximateAggregationQueryCostWithTimeRange(workloadPartition[i].getRecords(), replicas[i].getMeasurements(), replicas[i].getAverageChunkSize());
					if (newCost < replicaCost[i]) {
						replicaCost[i] = newCost;
					} else {
						replicas[i].swapMeasurementPos(swapLeft, swapRight);
					}
				}
				curCost = replicaCost[0];
				for(int i = 0; i < replicaNum; ++i) if(curCost < replicaCost[i]) curCost = replicaCost[i];
				if (!costRecorder.addCost(curCost, System.currentTimeMillis() - startTime)) {
					LOGGER.info("Break by cost recorder");
					break;
				}
				if (k % 200 == 0) LOGGER.info(String.format("SCOA Epoch %d: curCost %.3f", k, curCost));
				continue;
			}
			if (operation == 0) {
				// Swap chunk order
				int swapLeft = r.nextInt(measurementOrder.size());
				int swapRight = r.nextInt(measurementOrder.size());
				while (swapLeft == swapRight) {
					swapLeft = r.nextInt(measurementOrder.size());
					swapRight = r.nextInt(measurementOrder.size());
				}
//				float preCost = CostModel.approximateAggregationQueryCostWithTimeRange(workloadPartition[selectedReplica].getRecords(), replicas[selectedReplica].getMeasurements(), replicas[selectedReplica].getAverageChunkSize());
				replicas[selectedReplica].swapMeasurementPos(swapLeft, swapRight);
//				float afterCost = CostModel.approximateAggregationQueryCostWithTimeRange(workloadPartition[selectedReplica].getRecords(), replicas[selectedReplica].getMeasurements(), replicas[selectedReplica].getAverageChunkSize());
//				float probability = r.nextFloat();
//				probability = probability < 0 ? -probability : probability;
//				probability %= 1.0f;
//				if (preCost < afterCost || Math.exp((preCost - afterCost) / temperature) > probability) {
//					Pair<Float, Workload[]> costAndWorkloadPartition = getCostAndWorkloadPartitionForCurReplicas(records, replicas);
//					curCost = costAndWorkloadPartition.left;
//					workloadPartition = costAndWorkloadPartition.right;
//				} else {
//					replicas[selectedReplica].swapMeasurementPos(swapLeft, swapRight);
//				}
				Pair<Float, Workload[]> costAndWorkloadPartition = getCostAndWorkloadPartitionForCurReplicas(records, replicas);
				double newCost = costAndWorkloadPartition.left;

				float probability = r.nextFloat();
				probability = probability < 0 ? -probability : probability;
				probability %= 1.0f;
				if (newCost < curCost ||
								Math.exp((curCost - newCost) / temperature) > probability) {
					curCost = newCost;
					workloadPartition = costAndWorkloadPartition.right;
				} else {
					replicas[selectedReplica].swapMeasurementPos(swapLeft, swapRight);
				}
			} else {
				// Change chunk size
				long newChunkSize = Math.abs(r.nextLong());
				newChunkSize = newChunkSize % (chunkUpperBound - chunkLowerBound) + chunkLowerBound;
				long curChunkSize = replicas[selectedReplica].getAverageChunkSize();
				replicas[selectedReplica].setAverageChunkSize(newChunkSize);
				Pair<Float, Workload[]> costAndWorkloadPartition = getCostAndWorkloadPartitionForCurReplicas(records, replicas);
				double newCost = costAndWorkloadPartition.left;
				float probability = r.nextFloat();
				probability = probability < 0 ? -probability : probability;
				probability %= 1.0f;
				if (newCost < curCost ||
								Math.exp((curCost - newCost) / temperature) > probability) {
					curCost = newCost;
					workloadPartition = costAndWorkloadPartition.right;
				} else {
					replicas[selectedReplica].setAverageChunkSize(curChunkSize);
				}
//				if(k%200 == 0) {
//					LOGGER.info("ChunkSize");
//				}
			}
//			float minCost = Float.MAX_VALUE;
//			float maxCost = -1;
//			int minIdx = 0;
//			int maxIdx = 0;
//			for(int i = 0; i < replicaNum; ++i) {
//				float c = 0;
//				c = CostModel.approximateAggregationQueryCostWithTimeRange(workloadPartition[i].getRecords(), replicas[i].getMeasurements(), replicas[i].getAverageChunkSize());
//				if (c > maxCost) {
//					maxIdx = i;
//					maxCost = c;
//				}
//				if (c < minCost) {
//					minIdx = i;
//					minCost = c;
//				}
//			}
			selectedReplica = r.nextInt(replicaNum);
			if (!costRecorder.addCost(curCost, System.currentTimeMillis() - startTime)) {
				LOGGER.info("Break by cost recorder");
				break;
			}
			if (k % 200 == 0) LOGGER.info(String.format("Epoch %d: curCost %.3f", k, curCost));
		}
		LOGGER.info("Final cost: " + curCost);
		LOGGER.info("Loop count: " + k);
		return costRecorder.getLists();
	}

	public Pair<List<Double>, List<Long>> optimizeBySAWithChunkSizeAdjustmentSeparately() {
		double curCost = getCostAndWorkloadPartitionForCurReplicas(records, replicas).left;
		LOGGER.info("Ori cost: " + curCost);
		Pair<Long, Long> chunkBound = getChunkSizeBound(records);
		long chunkLowerBound = chunkBound.left;
		long chunkUpperBound = chunkBound.right;
		float temperature = SA_INIT_TEMPERATURE;
		long optimizeStartTime = System.currentTimeMillis();
		Random r = new Random();
		Workload[] workloadPartition = null;
		int k = 0;
		List<Double> costList = new ArrayList<>();
		List<Long> timeList = new ArrayList<>();
		long startTime = System.currentTimeMillis();
		boolean adjustChunkSize = false;
		for (; k < maxIter && System.currentTimeMillis() - optimizeStartTime < 6l * 60l * 1000l; ++k) {
			temperature = temperature * COOLING_RATE;
			int selectedReplica = r.nextInt(replicaNum);
			if (k < maxIter / 2 && System.currentTimeMillis() - optimizeStartTime < 3l * 60l * 1000l) {
				// Swap chunk order
				if (k % 500 == 0) {
					LOGGER.info("Adjusting column order");
				}
				int swapLeft = r.nextInt(measurementOrder.size());
				int swapRight = r.nextInt(measurementOrder.size());
				while (swapLeft == swapRight) {
					swapLeft = r.nextInt(measurementOrder.size());
					swapRight = r.nextInt(measurementOrder.size());
				}
				replicas[selectedReplica].swapMeasurementPos(swapLeft, swapRight);
				Pair<Float, Workload[]> costAndWorkloadPartition = getCostAndWorkloadPartitionForCurReplicas(records, replicas);
				double newCost = costAndWorkloadPartition.left;
				workloadPartition = costAndWorkloadPartition.right;
				float probability = r.nextFloat();
				probability = probability < 0 ? -probability : probability;
				probability %= 1.0f;
				if (newCost < curCost ||
								Math.exp((curCost - newCost) / temperature) > probability) {
					curCost = newCost;
				} else {
					replicas[selectedReplica].swapMeasurementPos(swapLeft, swapRight);
				}
			} else {
				// Change chunk size
				if (!adjustChunkSize) {
					adjustChunkSize = true;
					temperature = SA_INIT_TEMPERATURE;
				}
				if (k % 500 == 0) {
					LOGGER.info("Adjusting chunk size");
				}
				long newChunkSize = Math.abs(r.nextLong());
				newChunkSize = newChunkSize % (chunkUpperBound - chunkLowerBound) + chunkLowerBound;
				long curChunkSize = replicas[selectedReplica].getAverageChunkSize();
				replicas[selectedReplica].setAverageChunkSize(newChunkSize);
				Pair<Float, Workload[]> costAndWorkloadPartition = getCostAndWorkloadPartitionForCurReplicas(records, replicas);
				double newCost = costAndWorkloadPartition.left;
				workloadPartition = costAndWorkloadPartition.right;
				float probability = r.nextFloat();
				probability = probability < 0 ? -probability : probability;
				probability %= 1.0f;
				if (newCost < curCost ||
								Math.exp((curCost - newCost) / temperature) > probability) {
					curCost = newCost;
				} else {
					replicas[selectedReplica].setAverageChunkSize(curChunkSize);
				}
			}
			costList.add(curCost);
			timeList.add(System.currentTimeMillis() - startTime);
			if (k % 1000 == 0) {
				LOGGER.info(String.format("Epoch %d: curCost %.3f", k, curCost));
			}
		}
		LOGGER.info("Final cost: " + curCost);
		LOGGER.info("Loop count: " + k);
		return new Pair<>(costList, timeList);
	}

	public Pair<Replica[], Workload[]> optimizeByGAWithChunkSizeAdjustment() {

		MultiReplica[] curGeneration = GAInit();
		while (true) {
			if (isGAEnd(curGeneration) || GALoop > maxIter) break;
			curGeneration = GAPick(curGeneration);
			curGeneration = GACrossover(curGeneration);
			curGeneration = GAMutate(curGeneration);
			GALoop++;
		}
		MultiReplica best = getBest(curGeneration);
		return new Pair<>(best.getReplicas(), getWorkloadPartition(best));
	}

	private MultiReplica[] GAInit() {
		MultiReplica[] generation = new MultiReplica[GA_GENERATION_NUM];
		for (int i = 0; i < generation.length; ++i) {
			generation[i] = new MultiReplica(deviceID);
			generation[i].randomInit();
		}
		return generation;
	}

	private MultiReplica[] GAPick(MultiReplica[] generation) {
		float[] score = new float[generation.length];
		float totalScore = 0.0f;
		float bestScore = 0.0f;
		for (int i = 0; i < score.length; ++i) {
			score[i] = GAEstimate(generation[i]);
			if (score[i] > bestScore) {
				bestScore = score[i];
			}
			totalScore += score[i];
		}
		if (GALoop % 50 == 0) {
			LOGGER.info(String.format("Epoch %d: %.3f", GALoop, 10000.0f / bestScore));
		}
		float[] prob = new float[generation.length];
		for (int i = 0; i < score.length; ++i) {
			prob[i] = score[i] / totalScore;
		}
		for (int i = 1; i < prob.length; ++i) {
			prob[i] += prob[i - 1];
		}
		MultiReplica[] nextGeneration = new MultiReplica[generation.length];
		Random r = new Random();
		for (int i = 0; i < nextGeneration.length; ++i) {
			float curProb = Math.abs(r.nextFloat()) % 1.0f;
			for (int j = prob.length - 1; j >= 0; --j) {
				if (curProb >= prob[j]) {
					nextGeneration[i] = new MultiReplica(generation[i]);
					break;
				}
			}
			if (nextGeneration[i] == null) nextGeneration[i] = new MultiReplica(generation[0]);
		}
		return nextGeneration;
	}

	private MultiReplica[] GACrossover(MultiReplica[] generation) {
		int maxNum = (int) Math.ceil(GA_CROSS_RATE * generation.length) / 2;
		List<MultiReplica> list = new ArrayList<>();
		Collections.addAll(list, generation);
		Collections.shuffle(list);
		generation = list.toArray(new MultiReplica[0]);
		int length = generation.length;
		for (int i = 0; i < maxNum; ++i) {
			Pair<MultiReplica, MultiReplica> p = crossover(generation[i], generation[length - i - 1]);
			generation[i] = p.left;
			generation[length - i - 1] = p.right;
		}
		return generation;
	}

	private Pair<MultiReplica, MultiReplica> crossover(MultiReplica m1, MultiReplica m2) {
		int crossPoint = new Random().nextInt(m1.getReplicas().length);
		Replica[] replicasOfm1 = m1.getReplicas();
		Replica[] replicasOfm2 = m2.getReplicas();
		for (int i = crossPoint; i < replicasOfm1.length; ++i) {
			Replica temp = new Replica(replicasOfm1[i]);
			replicasOfm1[i] = new Replica(replicasOfm2[i]);
			replicasOfm2[i] = new Replica(temp);
		}
		MultiReplica newM1 = new MultiReplica(deviceID, replicasOfm1);
		MultiReplica newM2 = new MultiReplica(deviceID, replicasOfm2);
		return new Pair<>(newM1, newM2);
	}

	private MultiReplica[] GAMutate(MultiReplica[] generation) {
		int mutateNum = (int) (GA_MUTATE_RATE * generation.length);
		Random r = new Random();
		for (int i = 0; i < mutateNum; ++i) {
			int mutateIdx = r.nextInt(generation.length);
			generation[mutateIdx] = mutate(generation[mutateIdx]);
		}
		return generation;
	}

	private MultiReplica mutate(MultiReplica replica) {
		Random r = new Random();
		int mutatePos = r.nextInt(replica.getReplicas().length);
		Replica[] replicas = replica.getReplicas();
		List<String> newMeasurementOrder = new ArrayList<>(replicas[mutatePos].getMeasurements());
		Collections.shuffle(newMeasurementOrder);
		replicas[mutatePos] = new Replica(replicas[mutatePos].getDeviceId(), newMeasurementOrder, replicas[mutatePos].getAverageChunkSize());
		return new MultiReplica(replica.getDeviceID(), replicas);
	}

	private float GAEstimate(MultiReplica individual) {
		Replica[] replicas = individual.getReplicas();
		return 10000.0f / getCostAndWorkloadPartitionForCurReplicas(records, replicas).left;
	}

	private boolean isGAEnd(MultiReplica[] generation) {
		return false;
	}

	private MultiReplica getBest(MultiReplica[] generation) {
		MultiReplica bestReplicas = null;
		float minCost = Float.MAX_VALUE;
		for (MultiReplica individual : generation) {
			float curCost = GAEstimate(individual);
			if (curCost < minCost) {
				minCost = curCost;
				bestReplicas = individual;
			}
		}
		return bestReplicas;
	}

	private Workload[] getWorkloadPartition(MultiReplica multiReplica) {
		return null;
	}

	private Pair<Float, Workload[]> getCostAndWorkloadPartitionForCurReplicas(List<QueryRecord> records, Replica[] replicas) {
		float totalCost = 0.0f;
		Workload[] workloads = new Workload[replicaNum];
		for (int i = 0; i < replicaNum; ++i) {
			workloads[i] = new Workload();
		}
		List<Integer> indexes = new ArrayList<>();
		for (int i = 0; i < replicaNum; ++i) {
			indexes.add(i);
		}
		float[] workloadCost = new float[replicaNum];
		for(int i = 0; i < replicaNum; ++i) {
			workloadCost[i] = 0.0f;
		}
		Collections.shuffle(indexes);
		for (QueryRecord record : records) {
			List<QueryRecord> tmpList = new ArrayList<>();
			tmpList.add(record);
			float minCost = Float.MAX_VALUE;
			int minIdx = 0;
			Collections.shuffle(indexes);
			for (int i = 0; i < replicaNum; ++i) {
				Replica replica = replicas[indexes.get(i)];
				float curCost = CostModel.approximateAggregationQueryCostWithTimeRange(tmpList, replica.getMeasurements(), replica.getAverageChunkSize());
				if (Math.abs(curCost - minCost) > BALANCE_FACTOR * curCost && workloadCost[i] <= workloadCost[minIdx]) {
					minCost = curCost;
					minIdx = indexes.get(i);
				}
//				if (curCost < minCost) {
//					minIdx = indexes.get(i);
//				}
			}
			workloadCost[minIdx] += minCost;
			workloads[minIdx].addRecord(record);
		}
		for (int i = 0; i < replicaNum; ++i) {
			float curCost = CostModel.approximateAggregationQueryCostWithTimeRange(workloads[i].getRecords(), replicas[i].getMeasurements(), replicas[i].getAverageChunkSize());
			if (curCost > totalCost) {
				totalCost = curCost;
			}
		}
		return new Pair<>(totalCost, workloads);
	}

	public List<Double> getCostList() {
		return costList;
	}

	/**
	 * Optimize the chunk size
	 */
	public static Pair<List<Long>, List<Double>> chunkSizeOptimize(GroupByQueryRecord query, List<String> measurementOrder) {
		int visitLength = (int) query.getVisitLength();
		long visitChunkSize = MeasurePointEstimator.getInstance().getChunkSize(visitLength);
		long chunkSizeUpperBound = (long) (70.0f * visitChunkSize);
		long chunkSizeLowerBound = (long) (0.1f * visitChunkSize);
		long chunkSizeStep = (chunkSizeUpperBound - chunkSizeLowerBound) / CHUNK_SIZE_STEP_NUM;
		List<Long> chunkSizeList = new LinkedList<>();
		List<Double> costList = new LinkedList<>();
		for (long i = 0; i < CHUNK_SIZE_STEP_NUM; ++i) {
			long curChunkSize = chunkSizeLowerBound + chunkSizeStep * i;
			float curCost = CostModel.approximateSingleAggregationQueryCostWithTimeRange(query, measurementOrder, curChunkSize);
			chunkSizeList.add(curChunkSize);
			costList.add((double) curCost);
		}
		return new Pair<>(chunkSizeList, costList);
	}

	private Pair<Long, Long> getChunkSizeBound(List<QueryRecord> records) {
		long lowerBound = Long.MAX_VALUE;
		long upperBound = Long.MIN_VALUE;
		for (QueryRecord record : records) {
			if (record instanceof GroupByQueryRecord) {
				long visitLength = ((GroupByQueryRecord) record).getVisitLength();
				long visitChunkSize = MeasurePointEstimator.getInstance().getChunkSize(visitLength);
				if (visitChunkSize * CHUNK_SIZE_LOWER_BOUND < lowerBound) {
					lowerBound = (long) (visitChunkSize * CHUNK_SIZE_LOWER_BOUND);
				}
				if (visitChunkSize * CHUNK_SIZE_UPPER_BOUND > upperBound) {
					upperBound = (long) (visitChunkSize * CHUNK_SIZE_UPPER_BOUND);
				}
			}
		}

		return new Pair<>(lowerBound, upperBound);
	}

	public Pair<Replica[], Workload[]> optimizeBySAWithRainbow() {
		for (Replica replica : replicas) {
			replica.setAverageChunkSize(MeasurePointEstimator.getInstance().getChunkSize(30000));
		}
		double curCost = getCostAndWorkloadPartitionForCurReplicas(records, replicas).left;
		LOGGER.info("Ori cost: " + curCost);
		Pair<Long, Long> chunkBound = getChunkSizeBound(records);
		long chunkLowerBound = chunkBound.left;
		long chunkUpperBound = chunkBound.right;
		float temperature = SA_INIT_TEMPERATURE;
		long optimizeStartTime = System.currentTimeMillis();
		Random r = new Random();
		Workload[] workloadPartition = null;
		int k = 0;
		for (; k < maxIter && System.currentTimeMillis() - optimizeStartTime < 45l * 60l * 1000l; ++k) {
			temperature = temperature * COOLING_RATE;
			int selectedReplica = r.nextInt(replicaNum);
			int swapLeft = r.nextInt(measurementOrder.size());
			int swapRight = r.nextInt(measurementOrder.size());
			while (swapLeft == swapRight) {
				swapLeft = r.nextInt(measurementOrder.size());
				swapRight = r.nextInt(measurementOrder.size());
			}
			replicas[selectedReplica].swapMeasurementPos(swapLeft, swapRight);
			Pair<Float, Workload[]> costAndWorkloadPartition = getCostAndWorkloadPartitionForCurReplicas(records, replicas);
			double newCost = costAndWorkloadPartition.left;
			workloadPartition = costAndWorkloadPartition.right;
			float probability = r.nextFloat();
			probability = probability < 0 ? -probability : probability;
			probability %= 1.0f;
			if (newCost < curCost ||
							Math.exp((curCost - newCost) / temperature) > probability) {
				curCost = newCost;
			} else {
				replicas[selectedReplica].swapMeasurementPos(swapLeft, swapRight);
			}
			costList.add(curCost);
			if (k % 500 == 0) {
				LOGGER.info(String.format("Epoch %d: curCost %.3f", k, curCost));
			}
		}
		LOGGER.info("Final cost: " + curCost);
		LOGGER.info("Loop count: " + k);
		return new Pair<>(replicas, workloadPartition);
	}

	public List<Double> testWorkloadAdaption() {
//		List<QueryRecord> firstPart = new ArrayList<>();
//		List<QueryRecord> secondPart = new ArrayList<>();
//		long minVisitLength = Long.MAX_VALUE;
//		long maxVisitLength = Long.MIN_VALUE;
//		for(int i = 0; i < records.size(); ++i) {
//			if (((GroupByQueryRecord)records.get(i)).getVisitLength() < 500l) {
//				firstPart.add(records.get(i));
//			} else if (((GroupByQueryRecord)records.get(i)).getVisitLength() > 3000l &&
//							((GroupByQueryRecord)records.get(i)).getVisitLength() < 5000l) {
//				secondPart.add(records.get(i));
//			}
//		}
//		this.records = secondPart;
//		Pair<Replica[], List<Double>> initOptimize = optimizeBySAWithChunkSizeAdjustment();
//		List<Double> costList = new ArrayList<>();
//		for(int i = 0; i < initOptimize.right.size(); ++i) {
//			costList.add(initOptimize.right.get(i) / initOptimize.right.get(0));
//		}
//		double costFirst = (double)CostModel.approximateAggregationQueryCostWithTimeRange(firstPart,
//						initOptimize.left[0].getMeasurements(), initOptimize.left[0].getAverageChunkSize());
//		double costSecond = (double)CostModel.approximateAggregationQueryCostWithTimeRange(secondPart,
//						initOptimize.left[0].getMeasurements(), initOptimize.left[0].getAverageChunkSize());
//		for(int i = 0; i < initOptimize.right.size(); ++i) {
//			costList.add(costSecond / initOptimize.right.get(0));
//		}
//		for(int i = 0; i < initOptimize.right.size() / 2; ++i) {
//			double percentage = ((double)i) / ((double)initOptimize.right.size());
//			costList.add(costSecond / initOptimize.right.get(0) * (1.0-percentage) + 1 * percentage);
//		}
//		this.records = firstPart;
//		Pair<Replica[], List<Double>> secondOptimize = optimizeBySAWithChunkSizeAdjustment();
//		for(int i = 0; i < secondOptimize.right.size(); ++i) {
//			costList.add(secondOptimize.right.get(i) / secondOptimize.right.get(0));
//		}
////		double lastCost = costList.get(costList.size() - 1);
////		for(int i = 0; i < secondOptimize.right.size(); ++i) {
////			costList.add(lastCost / secondOptimize.right.get(0));
////		}
//		return costList;
		return null;
	}

	public static class CostRecorder {
		List<Double> costHistory;
		LinkedList<Double> recentCost;
		List<Long> timeList;
		boolean initMinMax;
		final int RECENT_RANGE = 2000;
		double maxCost;
		double minCost;
		CostRecorder() {
			costHistory = new ArrayList<>();
			recentCost = new LinkedList<>();
			timeList = new ArrayList<>();
			maxCost = Double.MIN_VALUE;
			minCost = Double.MAX_VALUE;
			initMinMax = false;
		}

		public boolean addCost(double cost, long time) {
			timeList.add(time);
			costHistory.add(cost);
			if (recentCost.size() >= RECENT_RANGE) {
				update(cost);
//				if ((maxCost - minCost) < maxCost * 0.01) {
//					return false;
//				}
				return true;
			} else {
				recentCost.addLast(cost);
				return true;
			}
		}

		private void update(double newCost) {
			if (!initMinMax) {
				initMinMax = true;
				updateMax();
				updateMin();
			}
			double first = recentCost.pop();
			recentCost.add(newCost);
			updateMax();
			updateMin();
		}

		private void updateMax() {
			minCost = Double.MAX_VALUE;
			for(int i = 0; i < recentCost.size(); ++i) {
				if (recentCost.get(i) < minCost) {
					minCost = recentCost.get(i);
				}
			}
		}

		private void updateMin() {
			maxCost = Double.MIN_VALUE;
			for (int i = 0; i < recentCost.size(); ++i) {
				if (recentCost.get(i) > maxCost) {
					maxCost = recentCost.get(i);
				}
			}
		}

		public Pair<List<Double>, List<Long>> getLists() {
			return new Pair<>(costHistory, timeList);
		}
	}
}
