package org.apache.iotdb.db.index.read.optimize;

/**
 * In the indexing mechanism, it refers to the optimization of the access order of candidate series
 * after pruning phase. Due to the mismatch between the candidate set order given by the index and
 * the file organization of TsFile, the access of the refinement phase advised by the index may be
 * inefficient. We can use this query optimizerto rearrange the candidate set access order.
 */
public interface IIndexCandidateOrderOptimize {
  class Factory {

    private Factory() {
      // hidden initializer
    }

    public static IIndexCandidateOrderOptimize getOptimize() {
      return new NoCandidateOrderOptimizer();
    }
  }
}
