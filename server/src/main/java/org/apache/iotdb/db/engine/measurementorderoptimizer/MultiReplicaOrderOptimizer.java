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
	private int maxIter = 5000000;
	private float breakPoint = 1e-2f;
	private static final Logger LOGGER = LoggerFactory.getLogger(MultiReplicaOrderOptimizer.class);
	private String deviceID;
	private Replica[] replicas;
	private List<String> measurementOrder;
	private List<QueryRecord> records;
	private List<Long> chunkSize;
	private final float SA_INIT_TEMPERATURE = 100.0f;
	private final float COOLING_RATE = 0.999f;
	private List<Double> costList = new LinkedList<>();
	private static long CHUNK_SIZE_STEP_NUM = 70000l;
	private final float CHUNK_SIZE_LOWER_BOUND = 0.8f;
	private final float CHUNK_SIZE_UPPER_BOUND = 2.0f;
	private final int GA_GENERATION_NUM = 50;
	private final float GA_CROSS_RATE = 0.15f;
	private final float GA_MUTATE_RATE = 0.10f;
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

	public Pair<Replica[], Workload[]> optimizeBySA() {
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
		for (; k < maxIter && System.currentTimeMillis() - optimizeStartTime < 60l * 60l * 1000l; ++k) {
			temperature = temperature * COOLING_RATE;
			int selectedReplica = r.nextInt(replicaNum);
			int operation = r.nextInt(2);
			if (operation == 0) {
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
			costList.add(curCost);
			if (k % 1000 == 0) {
				LOGGER.info(String.format("Epoch %d: curCost %.3f", k, curCost));
			}
		}
		LOGGER.info("Final cost: " + curCost);
		LOGGER.info("Loop count: " + k);
		return new Pair<>(replicas, workloadPartition);
	}

	public Pair<List<Double>, List<Long>> optimizeBySAWithChunkSizeAdjustmentAndCostRecord() {
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
		for (; k < maxIter && System.currentTimeMillis() - optimizeStartTime < 50l * 60l * 1000l; ++k) {
			temperature = temperature * COOLING_RATE;
			int selectedReplica = r.nextInt(replicaNum);
			int operation = r.nextInt(2);
			if (operation == 0) {
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
		for (; k < maxIter && System.currentTimeMillis() - optimizeStartTime < 20l * 60l * 1000l; ++k) {
			temperature = temperature * COOLING_RATE;
			int selectedReplica = r.nextInt(replicaNum);
			if (k < maxIter / 2 && System.currentTimeMillis() - optimizeStartTime > 10l * 60l * 1000l) {
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
		for (QueryRecord record : records) {
			List<QueryRecord> tmpList = new ArrayList<>();
			tmpList.add(record);
			float minCost = Float.MAX_VALUE;
			int minIdx = 0;
			// Collections.shuffle(indexes);
			for (int i = 0; i < replicaNum; ++i) {
				Replica replica = replicas[i];
				float curCost = CostModel.approximateAggregationQueryCostWithTimeRange(tmpList, replica.getMeasurements(), replica.getAverageChunkSize());
				if (curCost < minCost) {
					minCost = curCost;
					minIdx = i;
				}
			}
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
				long visitChunkSize = MeasurePointEstimator.getInstance().getChunkSize((int) visitLength);
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
}
