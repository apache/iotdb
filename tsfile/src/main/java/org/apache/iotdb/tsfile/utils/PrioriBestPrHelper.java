package org.apache.iotdb.tsfile.utils;

import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.statistics.distribution.TruncatedNormalDistribution;

import java.util.Arrays;

public class PrioriBestPrHelper {
  static KLLSketchLazyEmptyForSimuCompact simuWorker = null,
      simuWorkerSmall1 = null,
      simuWorkerSmall2 = null;
  static int maxMemoryNum;
  static int[] initialCompactNum;
  static int MERGE_BUFFER_RATIO = 0, MULTI_QUANTILE = 1;

  public PrioriBestPrHelper(int maxMemoryNum, int queryN, int multi_quantile) {
    PrioriBestPrHelper.maxMemoryNum = maxMemoryNum;
    simuWorker = new KLLSketchLazyEmptyForSimuCompact(maxMemoryNum);
    initialCompactNum = simuWorker.simulateCompactNumGivenN(queryN);
    MULTI_QUANTILE = multi_quantile;
    simuWorkerSmall1 = new KLLSketchLazyEmptyForSimuCompact(maxMemoryNum / MULTI_QUANTILE);
    simuWorkerSmall2 = new KLLSketchLazyEmptyForSimuCompact(maxMemoryNum / MULTI_QUANTILE);
  }

  public PrioriBestPrHelper(
      int maxMemoryNum,
      int queryN,
      int[] firstPassCompactNum,
      int merge_buffer_ratio,
      int multi_quantile) {
    PrioriBestPrHelper.maxMemoryNum = maxMemoryNum;
    simuWorker =
        merge_buffer_ratio == 0
            ? new KLLSketchLazyEmptyForSimuCompact(maxMemoryNum)
            : new KLLSketchLazyEmptyForSimuCompact(
                maxMemoryNum * (merge_buffer_ratio - 1) / merge_buffer_ratio);
    initialCompactNum = Arrays.copyOf(firstPassCompactNum, firstPassCompactNum.length);
    MERGE_BUFFER_RATIO = merge_buffer_ratio;
    MULTI_QUANTILE = multi_quantile;
    simuWorkerSmall1 = new KLLSketchLazyEmptyForSimuCompact(maxMemoryNum / MULTI_QUANTILE);
    simuWorkerSmall2 = new KLLSketchLazyEmptyForSimuCompact(maxMemoryNum / MULTI_QUANTILE);
  }

  public PrioriBestPrHelper(
      int maxMemoryNum, int queryN, int merge_buffer_ratio, int multi_quantile) {
    PrioriBestPrHelper.maxMemoryNum = maxMemoryNum;
    simuWorker =
        merge_buffer_ratio == 0
            ? new KLLSketchLazyEmptyForSimuCompact(maxMemoryNum)
            : new KLLSketchLazyEmptyForSimuCompact(
                maxMemoryNum * (merge_buffer_ratio - 1) / merge_buffer_ratio);
    initialCompactNum = simuWorker.simulateCompactNumGivenN(queryN);
    MERGE_BUFFER_RATIO = merge_buffer_ratio;
    MULTI_QUANTILE = multi_quantile;
    simuWorkerSmall1 = new KLLSketchLazyEmptyForSimuCompact(maxMemoryNum / MULTI_QUANTILE);
    simuWorkerSmall2 = new KLLSketchLazyEmptyForSimuCompact(maxMemoryNum / MULTI_QUANTILE);
  }

  public double[] findBestPr(double errRelativeEps, double minErr, double maxErr, int queryN) {
    double l = minErr, r = maxErr, err1 = l + (r - l) * (1 - 0.618), err2 = l + (r - l) * 0.618;
    double evaluateP1 = evaluatePr(maxMemoryNum, 1 - err1, queryN);
    double evaluateP2 = evaluatePr(maxMemoryNum, 1 - err2, queryN);
    boolean p1Better = true;
    boolean lastDropL = false, lastDropR = false;
    while ((r - l) / (1 - l) >= errRelativeEps) {
      //
      // System.out.println("\t\t\tp1:"+(1-err1)+"\tV1:"+evaluateP1+"\t\t\tp2:"+(1-err2)+"\tV2:"+evaluateP2);
      if (lastDropL) {
        err1 = err2;
        evaluateP1 = evaluateP2;
        err2 = l + (r - l) * 0.618;
        evaluateP2 = evaluatePr(maxMemoryNum, 1 - err2, queryN);
      }
      if (lastDropR) {
        err2 = err1;
        evaluateP2 = evaluateP1;
        err1 = l + (r - l) * (1 - 0.618);
        evaluateP1 = evaluatePr(maxMemoryNum, 1 - err1, queryN);
      }
      if (evaluateP1 < evaluateP2) {
        p1Better = true;
        lastDropR = true;
        lastDropL = false;
        r = err2;
      } else {
        p1Better = false;
        lastDropL = true;
        lastDropR = false;
        l = err1;
      }
    }
    double[] result =
        (p1Better) ? new double[] {1 - err1, evaluateP1} : new double[] {1 - err2, evaluateP2};
    double evaluateMinErr = evaluatePr(maxMemoryNum, 1 - minErr, queryN);
    if (evaluateMinErr <= result[1]) return new double[] {1 - minErr, evaluateMinErr};
    else return result;
  }

  private static KLLSketchLazyEmptyForSimuCompact findASimulator(int n) {
    //    return simuWorkerSmall1; // similar efficiency...
    if (simuWorkerSmall1.cntN < simuWorkerSmall2.cntN)
      return (n >= simuWorkerSmall1.cntN && n < simuWorkerSmall2.cntN)
          ? simuWorkerSmall1
          : simuWorkerSmall2;
    else
      return (n >= simuWorkerSmall2.cntN && n < simuWorkerSmall1.cntN)
          ? simuWorkerSmall2
          : simuWorkerSmall1;
  }

  private static double simulateIteration(
      double casePr,
      double fixPr,
      int depth,
      int maxMemoryNum,
      int avgN,
      double sig2,
      long maxERR) {
    int memoryNumForEachQuantile = maxMemoryNum / MULTI_QUANTILE;
    if (avgN <= 0) return 0;
    if (avgN + maxERR <= memoryNumForEachQuantile) return 1.0;
    NormalDistribution normalDis = new NormalDistribution(avgN, Math.sqrt(sig2));
    double allFinishPr = normalDis.cumulativeProbability(memoryNumForEachQuantile);
    if (MULTI_QUANTILE > 1) allFinishPr = Math.pow(allFinishPr, MULTI_QUANTILE);
    //
    // System.out.println("\t\t\t\t?????\t\tavgN:"+avgN+"\t\tmaxMem:"+maxMemoryNum+"\t\tfinishPr:"+finishPr);
    double continuePr = 1 - allFinishPr;
    if (continuePr < 1e-5) return 1.0;
    TruncatedNormalDistribution tnd =
        TruncatedNormalDistribution.of(
            avgN, Math.sqrt(sig2), memoryNumForEachQuantile, Double.MAX_VALUE);
    int continueAvgN = (int) Math.ceil(tnd.getMean());
    //    if(MULTI_QUANTILE>1){
    //      continueAvgN=(int)tnd.inverseCumulativeProbability(1-Math.pow(0.5,MULTI_QUANTILE));
    //      if(fixPr==0.9995)System.out.println("\t\t\t\t\tsimu  continueAvgN:\t"+continueAvgN);
    //    }

    int[] conCompactNum = findASimulator(continueAvgN).simulateCompactNumGivenN(continueAvgN);
    double conSig2 = KLLSketchLazyEmptyForSimuCompact.getSig2(conCompactNum);
    long conMaxErr = KLLSketchLazyEmptyForSimuCompact.getMaxError(conCompactNum);

    //        System.out.println("\tdepth:"+depth+"
    // avgN:"+avgN+"\tfixPr:"+fixPr+"\t\tcontinuePr:"+continuePr+"\t\t\tconAvgN:"+continueAvgN+"\t\tconMaxERR:"+conMaxErr);

    double allSuccessPr = MULTI_QUANTILE == 1 ? fixPr : Math.pow(fixPr, 1.0 / MULTI_QUANTILE);
    int conSuccessN =
        conMaxErr == 0
            ? 1
            : KLLSketchLazyExactPriori.queryRankErrBoundGivenParameter(
                    conSig2, conMaxErr, allSuccessPr)
                * 2;
    int conFailN = (Math.min(continueAvgN, (int) conMaxErr * 2) - conSuccessN) / 2;
    //        System.out.println("\t\t\tconSuccessN:"+conSuccessN+"\t\tconFailN:"+conFailN);
    //        bestSimuIter = 1 + pr * simulateIteration(casePr * pr, pr, depth + 1, successN,
    // maxMemoryNum, succComNum)[0] + (1 - pr) * (1 + simulateIteration(casePr * (1 - pr), pr, depth
    // + 1, failN, maxMemoryNum, failComNum)[0]);
    double simuPrF =
        allFinishPr * 1.0
            + continuePr
                * (1.0
                    + fixPr
                        * simulateIteration(
                            casePr * fixPr,
                            fixPr,
                            depth + 1,
                            maxMemoryNum,
                            conSuccessN,
                            conSig2,
                            conMaxErr)
                    + (1 - fixPr)
                        * (1.0
                            + simulateIteration(
                                casePr * (1 - fixPr),
                                fixPr,
                                depth + 1,
                                maxMemoryNum,
                                conFailN,
                                conSig2,
                                conMaxErr)));
    return simuPrF;
  }

  public static double evaluatePrFromScratch(
      int maxMemoryNum, double fixPr, int detN, int merge_buffer_ratio, int multi_quantile) {
    int firstPassMemory =
        merge_buffer_ratio == 0
            ? maxMemoryNum
            : maxMemoryNum * (merge_buffer_ratio - 1) / merge_buffer_ratio;
    if (detN <= firstPassMemory) return 1.0;
    MULTI_QUANTILE = multi_quantile;
    simuWorker =
        (merge_buffer_ratio == 0)
            ? new KLLSketchLazyEmptyForSimuCompact(maxMemoryNum)
            : new KLLSketchLazyEmptyForSimuCompact(
                maxMemoryNum * (merge_buffer_ratio - 1) / merge_buffer_ratio);
    simuWorkerSmall1 = new KLLSketchLazyEmptyForSimuCompact(maxMemoryNum / MULTI_QUANTILE);
    simuWorkerSmall2 = new KLLSketchLazyEmptyForSimuCompact(maxMemoryNum / MULTI_QUANTILE);
    int maxERR =
        KLLSketchLazyExactPriori.queryRankErrBound(simuWorker.simulateCompactNumGivenN(detN), 1.0);
    double sig2 = simuWorker.getSig2();
    long nextMaxErr = simuWorker.getMaxError();
    double allSuccessPr = MULTI_QUANTILE == 1 ? fixPr : Math.pow(fixPr, 1.0 / MULTI_QUANTILE);
    int prERR = KLLSketchLazyExactPriori.queryRankErrBound(simuWorker.compactNum, allSuccessPr);

    int successAvgN = prERR * 2;
    int failAvgN = (Math.min(detN, maxERR * 2) - successAvgN) / 2;

    //        System.out.println("\t\tfirst paas.
    // successAvgN:\t"+successAvgN+"\t\tfailAvgN:\t"+failAvgN);
    //        System.out.println("\t\t\t\t\t"+sig2+"\t"+maxERR+"\t\t\tnextMaxERR:\t"+nextMaxErr);

    double simulateResult;
    double successResult =
        simulateIteration(fixPr, fixPr, 1, maxMemoryNum, successAvgN, sig2, nextMaxErr);
    double failResult =
        simulateIteration((1 - fixPr), fixPr, 1, maxMemoryNum, failAvgN, sig2, nextMaxErr);
    simulateResult = 1.0 + fixPr * successResult + (1 - fixPr) * (1.0 + failResult);
    //
    // if(DEBUG_PRINT)System.out.println("\t\t\t\t\t\t\t\tcntPR:"+fixPr+"\tsuccessN:\t"+succN+"\t\tfailN:\t"+failN+/*"\t\testi_iter:\t"+estimateIterationNum+*/"\t\tsimu_iter:\t"+simulateResult[0]+"\tsimu_nextSuccessPr:"+simulateResult[1]);
    return simulateResult;
  }

  private static double evaluatePr(int maxMemoryNum, double fixPr, int detN) {
    if (MERGE_BUFFER_RATIO == 0 && detN <= maxMemoryNum) return 1.0;
    int maxERR = KLLSketchLazyExactPriori.queryRankErrBound(initialCompactNum, 1.0);
    double sig2 = KLLSketchLazyEmptyForSimuCompact.getSig2(initialCompactNum);
    long nextMaxErr = KLLSketchLazyEmptyForSimuCompact.getMaxError(initialCompactNum);
    double allSuccessPr = MULTI_QUANTILE == 1 ? fixPr : Math.pow(fixPr, 1.0 / MULTI_QUANTILE);
    //
    // if(MULTI_QUANTILE>1&&allSuccessPr!=fixPr)System.out.println("\t\t\t\t\t"+allSuccessPr+"\t\t"+fixPr);
    int prERR = KLLSketchLazyExactPriori.queryRankErrBound(initialCompactNum, allSuccessPr);

    int successAvgN = prERR * 2;
    // if(fixPr==0.9995)System.out.println("\t\t\tsuccAvgN:\t"+successAvgN+"\t\tallSuccessPr:\t"+allSuccessPr);
    int failAvgN = (Math.min(detN, maxERR * 2) - successAvgN) / 2;

    //        System.out.println("\t\tfirst paas.
    // successAvgN:\t"+successAvgN+"\t\tfailAvgN:\t"+failAvgN+"\t\tfixPr:"+fixPr);
    //        System.out.println("\t\t\t\t\t"+sig2+"\t"+maxERR+"\t\t\tnextMaxERR:\t"+nextMaxErr);

    double simulateResult;
    double successResult =
        simulateIteration(fixPr, fixPr, 1, maxMemoryNum, successAvgN, sig2, nextMaxErr);
    double failResult =
        simulateIteration((1 - fixPr), fixPr, 1, maxMemoryNum, failAvgN, sig2, nextMaxErr);
    simulateResult = 1.0 + fixPr * successResult + (1 - fixPr) * (1.0 + failResult);
    //
    // if(DEBUG_PRINT)System.out.println("\t\t\t\t\t\t\t\tcntPR:"+fixPr+"\tsuccessN:\t"+succN+"\t\tfailN:\t"+failN+/*"\t\testi_iter:\t"+estimateIterationNum+*/"\t\tsimu_iter:\t"+simulateResult[0]+"\tsimu_nextSuccessPr:"+simulateResult[1]);
    return simulateResult;
  }
}
