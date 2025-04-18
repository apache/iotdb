package org.apache.iotdb.library.dmatch.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class MatchingKey {
  int labelIndex;
  double min_support;
  double min_confidence;
  int columnLength;
  int totalPairs;
  int[] distanceMin;
  int[] distanceMax;
  List<TupleEntry> tupleList;
  Set<String> positivePairs;
  Set<String> negativePairs;
  Map<Integer, Set<Integer>> distanceMap;
  Map<String, int[]> distanceCache;
  Set<String> CandidateCache;

  public MatchingKey(int labelIndex, double min_support, double min_confidence) {
    this.labelIndex = labelIndex;
    this.min_support = min_support;
    this.min_confidence = min_confidence;
    this.totalPairs = 0;
    this.tupleList = new ArrayList<>();
    this.positivePairs = new HashSet<>();
    this.negativePairs = new HashSet<>();
    this.distanceMap = new HashMap<>();
    this.distanceCache = new HashMap<>();
    this.CandidateCache = new HashSet<>();
  }

  public void add(int index, long time, String[] fullTuple) {
    tupleList.add(new TupleEntry(index, time, fullTuple));
  }

  public Candidate computeAllPairs() {
    for (int i = 0; i < tupleList.size(); i++) {
      for (int j = i + 1; j < tupleList.size(); j++) {
        totalPairs++;
        TupleEntry t1 = tupleList.get(i);
        TupleEntry t2 = tupleList.get(j);
        int index = 0;
        int[] distances = new int[columnLength];
        int timeDiff = (int) Math.abs((t1.time - t2.time) / 1000);
        distances[index] = timeDiff;
        distanceMap.get(index).add(timeDiff);
        for (int k = 0; k < columnLength; k++) {
          if (k != labelIndex) {
            index++;
            int distance = editDistance(t1.tuple[k], t2.tuple[k]);
            distances[index] = distance;
            distanceMap.get(index).add(distance);
          }
        }
        String label1 = t1.tuple[labelIndex];
        String label2 = t2.tuple[labelIndex];
        String key = i + "+" + j;
        if (label1.equals(label2)) {
          positivePairs.add(key);
          distanceCache.put(key, distances);
        } else {
          negativePairs.add(key);
          distanceCache.put(key, distances);
        }
      }
    }
    List<int[]> distanceRestrictions = new ArrayList<>();
    for (int i = 0; i < distanceMap.size(); i++) {
      Set<Integer> distances = distanceMap.get(i);
      if (distances == null || distances.isEmpty()) {
        distanceRestrictions.add(new int[] {0, 0});
      } else {
        int min = Collections.min(distances);
        int max = Collections.max(distances);
        distanceRestrictions.add(new int[] {min, max});
        distanceMin[i] = min;
        distanceMax[i] = max;
      }
    }
    Candidate psi = new Candidate(distanceRestrictions, totalPairs);
    for (String pair : positivePairs) {
      psi.addpositive(pair);
    }
    for (String pair : negativePairs) {
      psi.addNegative(pair);
    }
    return psi;
  }

  private int editDistance(String s1, String s2) {
    if (s1 == null || s2 == null) return Integer.MAX_VALUE;
    int len1 = s1.length(), len2 = s2.length();
    int[][] dp = new int[len1 + 1][len2 + 1];
    for (int i = 0; i <= len1; i++) dp[i][0] = i;
    for (int j = 0; j <= len2; j++) dp[0][j] = j;

    for (int i = 1; i <= len1; i++) {
      for (int j = 1; j <= len2; j++) {
        int cost = (s1.charAt(i - 1) == s2.charAt(j - 1)) ? 0 : 1;
        dp[i][j] = Math.min(Math.min(dp[i - 1][j] + 1, dp[i][j - 1] + 1), dp[i - 1][j - 1] + cost);
      }
    }
    return dp[len1][len2];
  }

  public void setColumnLength(int l) {
    this.columnLength = l;
    this.distanceMax = new int[columnLength];
    this.distanceMin = new int[columnLength];
    for (int k = 0; k < columnLength; k++) {
      this.distanceMap.put(k, new TreeSet<>()); // 使用 TreeSet 保证自动排序
    }
  }

  public Set<Candidate> MS(Candidate psi) {
    Set<Candidate> PsiS = new HashSet<>();
    for (int attrIndex = 0; attrIndex < psi.getDistanceRestrictions().size(); attrIndex++) {
      int[] cPsi = psi.getDistanceRestrictions().get(attrIndex);
      int dv = cPsi[0];
      int du = cPsi[1];
      if (dv == du) continue;
      List<Integer> topDistances = new ArrayList<>(distanceMap.get(attrIndex));
      int dSMax = dv;
      int dSMin = du;
      if ((du - dv) != 1) {
        for (int d : topDistances) {
          if (d > dv) {
            dSMin = d;
            break;
          }
        }
        for (int i = topDistances.size() - 1; i >= 0; i--) {
          int d = topDistances.get(i);
          if (d < du) {
            dSMax = d;
            break;
          }
        }
      }

      List<int[]> restrictions1 = new ArrayList<>();
      for (int[] range : psi.getDistanceRestrictions()) {
        restrictions1.add(Arrays.copyOf(range, range.length));
      }
      restrictions1.set(attrIndex, new int[] {dv, dSMax});
      Candidate psi1 = new Candidate(restrictions1, totalPairs);
      PsiS.add(psi1);

      List<int[]> restrictions2 = new ArrayList<>();
      for (int[] range : psi.getDistanceRestrictions()) {
        restrictions2.add(Arrays.copyOf(range, range.length));
      }
      restrictions2.set(attrIndex, new int[] {dSMin, du});
      Candidate psi2 = new Candidate(restrictions2, totalPairs);
      PsiS.add(psi2);
    }
    Iterator<Candidate> iterator = PsiS.iterator();
    while (iterator.hasNext()) {
      Candidate psiS = iterator.next();
      String key = psiS.getSerial();
      if (CandidateCache.contains(key)) {
        iterator.remove();
        continue;
      }
      for (String pair : new HashSet<>(psi.getPositiveSet())) {
        if (psiS.matches(pair)) {
          psiS.addpositive(pair);
        }
      }
      if (psiS.getPositiveSetSize() == 0) {
        CandidateCache.add(key);
        iterator.remove();
      }
    }
    return PsiS;
  }

  public Set<Candidate> MCG(Candidate psi, Set<String> qMinusPsi0) {
    Set<Candidate> Psi0 = new HashSet<>();
    Psi0.add(psi);
    boolean label = false;
    Set<String> p = new HashSet<>(qMinusPsi0);
    for (String pair : qMinusPsi0) {
      Set<Candidate> PsiA = new HashSet<>();
      Set<Candidate> PsiR = new HashSet<>();
      if (psi.matches(pair)) {
        psi.addNegative(pair);
        double confidence = psi.getConfidence();
        if (Double.isNaN(confidence) || confidence < min_confidence || confidence == 0.0) {
          PsiR.add(psi);
          Set<Candidate> PsiS = MS(psi);
          for (Candidate psiS : PsiS) {
            String key = psiS.getSerial();
            if (!CandidateCache.contains(key)) {
              CandidateCache.add(key);
              PsiA.addAll(MCG(psiS, p));
            }
          }
          label = true;
        }
      } else {
        p.remove(pair);
      }
      for (Candidate psii : PsiA) {
        if (psii.getSupport() == 0) {
          PsiR.add(psii);
        }
      }
      Psi0.addAll(PsiA);
      Psi0.removeAll(PsiR);
      if (label) break;
    }
    for (Candidate psii : Psi0) {
      List<int[]> distanceRestrictions = psii.getDistanceRestrictions();

      for (int attrIndex = 0; attrIndex < distanceRestrictions.size(); attrIndex++) {
        int[] restrictions = distanceRestrictions.get(attrIndex);
        List<int[]> subsets =
            generateSubsets(restrictions, new ArrayList<>(distanceMap.get(attrIndex)));

        for (int[] subset : subsets) {
          List<int[]> newRestrictions = new ArrayList<>();
          for (int i = 0; i < distanceRestrictions.size(); i++) {
            if (i == attrIndex) {
              newRestrictions.add(subset);
            } else {
              newRestrictions.add(distanceRestrictions.get(i));
            }
          }
          String key = serializeRestrictions(newRestrictions);
          CandidateCache.add(key);
        }
      }
    }
    return Psi0;
  }

  public Set<Candidate> GAP(Set<Candidate> psi) {
    Set<Candidate> psio = new HashSet<>();
    double supp_s_psi_o = 0;
    while (!psi.isEmpty()) {
      Candidate bestCandidate = null;
      double maxSupport = -1;

      for (Candidate candidate : psi) {
        double currentSupport = candidate.getSupport();
        if (currentSupport > maxSupport) {
          maxSupport = currentSupport;
          bestCandidate = candidate;
        }
      }

      if (maxSupport == 0) {
        break;
      }

      Candidate bestCandidateCopy = bestCandidate.copy();
      psio.add(bestCandidateCopy);
      supp_s_psi_o += bestCandidateCopy.getSupport();
      psi.remove(bestCandidate);
      if (supp_s_psi_o >= min_support) {
        break;
      }

      Candidate psi_p1 = calculatePsiP1(bestCandidateCopy.getDistanceRestrictions());
      Candidate psi_p5 = calculatePsiP5(bestCandidateCopy.getDistanceRestrictions());
      Candidate psi_p6 = calculatePsiP6(bestCandidateCopy.getDistanceRestrictions());
      Iterator<Candidate> iterator = psi.iterator();
      while (iterator.hasNext()) {
        Candidate candidate = iterator.next();
        boolean shouldUpdate = false;

        List<int[]> distanceRestrictions = candidate.getDistanceRestrictions();
        for (int attrIndex = 0; attrIndex < distanceRestrictions.size(); attrIndex++) {
          int[] range = distanceRestrictions.get(attrIndex);
          int[] range_p1 = psi_p1.getDistanceRestrictions().get(attrIndex);
          int[] range_p5 = psi_p5.getDistanceRestrictions().get(attrIndex);
          int[] range_p6 = psi_p6.getDistanceRestrictions().get(attrIndex);

          if ((range_p1 != null && range[0] <= range_p1[0] && range[1] >= range_p1[1])
              || (range_p5 != null && range_p5[0] <= range[0] && range_p5[1] >= range[1])
              || (range_p6 != null && range_p6[0] <= range[0] && range_p6[1] >= range[1])) {
            shouldUpdate = true;
            break;
          }
        }

        if (shouldUpdate) {
          Set<String> bestCandidateAgreeSet = bestCandidateCopy.getAgreeSet();
          Set<String> candidateAgreeSet = new HashSet<>(candidate.getAgreeSet());

          for (String candidatePairKey : candidateAgreeSet) {
            if (bestCandidateAgreeSet.contains(candidatePairKey)) {
              candidate.deleteAgree(candidatePairKey);
            }
          }
        } else {
          iterator.remove();
        }
      }
    }

    if (supp_s_psi_o < min_support) {
      return Collections.emptySet();
    } else {
      return psio;
    }
  }

  private Candidate calculatePsiP1(List<int[]> originalRestrictions) {
    List<int[]> newRestrictions = new ArrayList<>();
    int i = 0;
    for (int[] range : originalRestrictions) {
      if (range[0] == distanceMin[i] || range[1] == distanceMax[i]) {
        newRestrictions.add(new int[] {-1, Integer.MAX_VALUE});
      } else {
        int[] newRange = new int[] {range[0] - 1, range[1] + 1};
        newRestrictions.add(newRange);
      }
      i = i + 1;
    }

    return new Candidate(newRestrictions, totalPairs);
  }

  private Candidate calculatePsiP5(List<int[]> originalRestrictions) {
    List<int[]> newRestrictions = new ArrayList<>();
    int i = 0;
    for (int[] range : originalRestrictions) {
      if (range[0] == distanceMin[i]) {
        newRestrictions.add(new int[] {-1, -1});
      } else {
        int[] newRange = new int[] {distanceMin[i], range[0] - 1};
        newRestrictions.add(newRange);
      }
      i = i + 1;
    }
    return new Candidate(newRestrictions, totalPairs);
  }

  private Candidate calculatePsiP6(List<int[]> originalRestrictions) {
    List<int[]> newRestrictions = new ArrayList<>();
    int i = 0;

    for (int[] range : originalRestrictions) {
      if (range[1] == distanceMax[i]) {
        newRestrictions.add(new int[] {Integer.MAX_VALUE, Integer.MAX_VALUE});
      } else {
        int[] newRange = new int[] {range[1] + 1, distanceMax[i]};
        newRestrictions.add(newRange);
      }
      i = i + 1;
    }
    return new Candidate(newRestrictions, totalPairs);
  }

  private String serializeRestrictions(List<int[]> restrictions) {
    StringBuilder sb = new StringBuilder();
    for (int[] r : restrictions) {
      sb.append(r[0]).append(r[1]);
    }
    return sb.toString();
  }

  public List<int[]> generateSubsets(int[] range, List<Integer> topDistances) {
    List<int[]> subsets = new ArrayList<>();

    for (int i = 0; i <= (topDistances.size() - 1); i++) {
      for (int j = i; j < topDistances.size(); j++) {
        if (topDistances.get(i) >= range[0] && topDistances.get(j) <= range[1]) {
          int[] subset = {topDistances.get(i), topDistances.get(j)}; // 构建当前子集
          subsets.add(subset);
        } else if (topDistances.get(i) < range[0] || topDistances.get(i) > range[1]) {
          break;
        }
      }
    }
    return subsets;
  }

  private static class TupleEntry {
    int index;
    long time;
    String[] tuple;

    TupleEntry(int index, long time, String[] tuple) {
      this.index = index;
      this.time = time;
      this.tuple = tuple;
    }
  }

  public class Candidate {
    private List<int[]> distanceRestrictions;
    private Set<String> agreeSet;
    private Set<String> positiveSet;
    private Set<String> negativeSet;
    private double support;
    private double confidence;
    private int totalPairs;
    private String serial;

    public Candidate(List<int[]> distanceRestrictions, int totalPairs) {
      this.distanceRestrictions = distanceRestrictions;
      this.positiveSet = new HashSet<>();
      this.agreeSet = new HashSet<>();
      this.negativeSet = new HashSet<>(); // Initialize negative set
      this.totalPairs = totalPairs;
      this.confidence = 0.0;
      this.support = 0.0;
      StringBuilder sb = new StringBuilder();
      for (int[] r : distanceRestrictions) {
        sb.append(r[0]).append(r[1]);
      }
      this.serial = sb.toString();
    }

    public void addAgree(String pairKey) {
      agreeSet.add(pairKey);
    }

    public String getSerial() {
      return this.serial;
    }

    public Candidate copy() {
      List<int[]> copiedRestrictions = new ArrayList<>();
      for (int[] range : this.distanceRestrictions) {
        copiedRestrictions.add(Arrays.copyOf(range, range.length));
      }
      Candidate candidateCopy = new Candidate(copiedRestrictions, this.totalPairs);
      for (String pair : this.agreeSet) {
        candidateCopy.addAgree(pair);
      }
      for (String pair : this.negativeSet) {
        candidateCopy.addNegative(pair);
      }
      for (String pair : this.positiveSet) {
        candidateCopy.addpositive(pair);
      }
      candidateCopy.confidence = this.confidence;
      candidateCopy.support = this.support;

      return candidateCopy;
    }

    public boolean matches(String pair) {
      int[] distances = distanceCache.get(pair);
      if (distances == null) {
        return false;
      }

      for (int attrIndex = 0; attrIndex < distanceRestrictions.size(); attrIndex++) {
        int[] restriction = distanceRestrictions.get(attrIndex);
        int distance = distances[attrIndex];

        if (distance < restriction[0] || distance > restriction[1]) {
          return false;
        }
      }
      return true;
    }

    public void deleteAgree(String pair) {
      agreeSet.remove(pair);
    }

    public void addNegative(String pair) {
      negativeSet.add(pair);
      agreeSet.add(pair);
    }

    public void addpositive(String pair) {
      positiveSet.add(pair);
      agreeSet.add(pair);
    }

    public double getConfidence() {
      this.confidence = (double) positiveSet.size() / (double) agreeSet.size();
      return confidence;
    }

    public double getSupport() {
      support = (double) agreeSet.size() / totalPairs;
      return support;
    }

    public List<int[]> getDistanceRestrictions() {
      return distanceRestrictions;
    }

    public Set<String> getAgreeSet() {
      return agreeSet;
    }

    public Set<String> getNegativeSet() {
      return negativeSet;
    }

    public Set<String> getPositiveSet() {
      return positiveSet;
    }

    public int getPositiveSetSize() {
      return positiveSet.size();
    }
  }
}
