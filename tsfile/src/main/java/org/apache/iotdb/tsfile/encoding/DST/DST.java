package org.apache.iotdb.tsfile.encoding.DST;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class DST {

    private List<Integer> factor(int k, boolean[] isprime) {
        List<Integer> ret = new ArrayList<>();
        if(k == 1) {
            ret.add(1);
            return ret;
        }
        int p = 2;
        while (k > 1) {
            while(!isprime[p] && p <= k) {
                p++;
            }
            while(k % p == 0) {
                ret.add(p);
                k /= p;
            }
            p++;
        }
        return ret;
    }

    private int gcd(int p, int q)
    {
        if(q==0)
            return p;
        int r = p%q;
        return gcd(q, r);
    }

    public int[][] diffSeq(int N) {

        boolean isprime[] = new boolean[N+1];
        for(int i = 1; i <=N; i++) {
            isprime[i] = true;
        }
        for(int i=2;i*i<=N;i++)
        {
            if(isprime[i])
            {
                int tmp = i+i;
                while(tmp<=N)
                {
                    isprime[tmp] = false;
                    tmp+=i;
                }
            }
        }
        isprime[1] = false;
        //prime selector------------------

        int W2[][] = new int[N+1][N+1];
        for (int i = 1; i <= N; i++) {
            for (int j = 1; j <= N; j++) {
                W2[i][j] = 0;
                if (j == 1) {
                    W2[i][j] = 1;
                }
            }
        }
        int a[] = {0, 1, -1};

        for (int k = 1; k <= N; k++) {
            if (k <= 2) {
                W2[k][2] = a[k];
            } else {
                if (k % 2 == 0) {
                    W2[k][2] = a[2];
                } else {
                    W2[k][2] = a[k%2];
                }
            }
        }

        for (int i = 3; i <= N; i++) {
            if(isprime[i]) {
                int b[] = new int[i - 1];
                int c[] = new int[i + 1];
                c[i - 1] = 1;
                c[i] = -1;
                for (int k = 1; k <= N; k++) {
                    if (k <= i)
                        W2[k][i] = c[k];
                    else {
                        if ((k) % (i) == 0)
                            W2[k][i] = c[i];
                        else
                            W2[k][i] = c[(k) % (i)];
                    }
                }
            } else {
                List<Integer> d = factor(i, isprime);
                d.add(0, 0);
                HashSet<Integer> unidt = new HashSet<>(d);
//                for(int dd : d) {
//                    System.out.print(dd);
//                    System.out.print(" ");
//                }
//                System.out.println("");
//                for(int dd : unidt) {
//                    System.out.print(dd);
//                    System.out.print(" ");
//                }
//                System.out.println("");
                List<Integer> unid = new ArrayList<>(unidt);
//                for(int dd : unid) {
//                    System.out.print(dd);
//                    System.out.print(" ");
//                }
//                System.out.println("");
                int lenu = unid.size() - 1;
                if (lenu == 1) {
                    int e[] = new int[i+1];

                    for (int k = 1; k <= d.get(1); k++) {
                        e[k+((k-1)*(i/d.get(1)-1))] = W2[k][d.get(1)];
                    }
//                    for(int t = 1; t <= i; t++) {
//                        System.out.print(e[t]);
//                        System.out.print(" ");
//                    }
//                    System.out.println("");

                    for (int k = 1; k <= N; k++) {
                        if(k <= i)
                            W2[k][i] = e[k];
                        else {
                            if (k%i == 0)
                                W2[k][i] = e[i];
                            else
                                W2[k][i] = e[k%i];
                        }
                    }
                }
                else {
                    int muld[] = new int[lenu+1];
                    for (int k = 1; k <= lenu; k++) {
                        int tempk = unid.get(k);
                        int lend = 0;
                        for (int t = 1; t < d.size(); t++)
                            if (d.get(t) == tempk)
                                lend++;
                        muld[k] = (int)Math.pow(tempk, lend);
                    }
                    int f[] = new int[i+1];
                    for (int k = 1; k <= i; k++)
                        f[k] = 1;
                    for (int k = 1; k <= lenu; k++) {
                        for (int t = 1; t <= i; t++) {
                            f[t] *= W2[t][muld[k]];
                        }
                    }
                    for (int k = 1; k <= N; k++) {
                        if(k <= i)
                            W2[k][i] = f[k];
                        else {
                            if (k%i == 0)
                                W2[k][i] = f[i];
                            else
                                W2[k][i] = f[k%i];
                        }
                    }
                }
            }
        }

        return W2;
    }

    public int[][] DSTmatrix(int N) {
        int z[][] = diffSeq(N);
        int F[][] = new int[N+1][N+1];
        int k = 1;
        for (int i = 1; i <= N; i++) {
            if(N%i == 0) {
                int X[] = new int[N + 1];
                for (int j = 1; j <= N; j++) {
                    F[j][k] = z[j][i];
                    X[j] = F[j][k];
                }
                int rp = 0;
                for (int j = 1; j <= i; j++) {
                    if (gcd(j, i) == 1)
                        rp++;
                }
                for (int j = 1; j <= rp - 1; j++) {
                    F[1][k + j] = X[N];
                    for (int t = 2; t <= N; t++) {
                        F[t][k + j] = X[t - 1];
                    }
                    for (int t = 1; t <= N; t++) {
                        X[t] = F[t][k + j];
                    }
                }
                k += rp;
            }
        }
        int FT[][] = new int[N+1][N+1];
        for (int i = 1; i <= N; i++) {
            for (int j = 1; j <= N; j++) {
                FT[i][j] = F[j][i];
            }
        }
        return FT;
    }

    public List<Integer> fit(List<Integer> org, int N) {
        int MT[][] = DSTmatrix(N);
        List<Integer> ret = new ArrayList<>();
        int s = org.size() / N + 1;
        for (int k = 0; k < s - 1; k++) {
            for (int i = 0; i < N; i++) {
                int tmp = 0;
                for (int j = 0; j < N; j++) {
                    tmp += MT[i+1][j+1]*org.get(k*N+j);
                }
                ret.add(tmp);
            }
        }
        for (int i = 0; i < org.size()-s*N; i++) {
            int tmp = 0;
            for (int j = 0; j < org.size()-s*N; j++) {
                tmp += MT[i+1][j+1]*org.get(s*N+j);
            }
            ret.add(tmp);
        }
        return ret;
    }

}

