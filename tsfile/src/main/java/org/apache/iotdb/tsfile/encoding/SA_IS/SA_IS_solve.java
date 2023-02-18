package org.apache.iotdb.tsfile.encoding.SA_IS;

public class SA_IS_solve {
  private int[] ss, id, nxt, ans;

  public SA_IS_solve() {
    return;
  }

  public int[] solve(int[] s) {
    final int M = 258;
    int n = s.length, start = 0;
    int[] cnt = new int[M];
    for (int i = 0; i < n; i++) cnt[s[i]]++;
    for (int i = 1; i < M; i++) cnt[i] += cnt[i - 1];
    ss = new int[n];
    id = new int[n];
    for (int i = 0; i < M; i++)
      for (int j = (i == 0 ? 0 : cnt[i - 1]); j < cnt[i]; j++) {
        ss[j] = i;
        id[j] = i - (i == 0 ? 0 : cnt[i - 1]);
      }
    int[] cnt2 = new int[M];
    nxt = new int[n];
    for (int i = 0; i < n; i++) {
      nxt[(s[i] == 0 ? 0 : cnt[s[i] - 1]) + cnt2[s[i]]] = i;
      cnt2[s[i]]++;
      if (s[i] == 0) start = i;
    }
    ans = new int[n];
    int idx = 0, pt = start;
    do {
      ans[idx++] = ss[pt];
      pt = nxt[pt];
    } while (pt != start);
    return ans;
  }
}
