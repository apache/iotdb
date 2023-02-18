package org.apache.iotdb.tsfile.encoding.BIT;

public class BIT {
  private int n, lgn;
  private int[] bit;

  public BIT(int _n) {
    if (_n <= 0) throw new Error("Invalid range n.");
    n = _n;
    lgn = 0;
    while (1 << (lgn + 1) <= n) lgn++;
    bit = new int[n + 1];
  }

  private int lowbit(int x) {
    return x & -x;
  }

  public void add(int x, int k) {
    if (x < 1 || x > n) return;
    while (x <= n) {
      bit[x] += k;
      x += lowbit(x);
    }
  }

  public int ask(int x) {
    if (x <= 0) return 0;
    if (x > n) x = n;
    int res = 0;
    while (x != 0) {
      res += bit[x];
      x &= x - 1;
    }
    return res;
  }

  public int find(int x) {
    int res = 0, cur = 0;
    for (int i = lgn; i >= 0; i--) {
      if ((res | 1 << i) <= n && cur + bit[res | 1 << i] >= x) {
        cur += bit[res | 1 << i];
        res |= 1 << i;
      }
    }
    return res;
  }
}
