package org.apache.iotdb.tsfile.encoding.SA_IS;

public class SA_IS {
    private static int[] rk,sa;
    private static int[] sum,cur;
    private static void induce(int[] a,int[] p,boolean[] tp,int n,int m) {
        for(int i=0;i<n;i++)
            sa[i]=-1;
        for(int i=0;i<n;i++)
            sum[i]=0;
        for(int i=0;i<n;i++)
            sum[a[i]]++;
        for(int i=1;i<n;i++)
            sum[i]+=sum[i-1];
        for(int i=0;i<n;i++)
            cur[i]=sum[i]-1;
        for(int i=m-1;i>=0;i--)
            sa[cur[a[p[i]]]--]=p[i];
        cur[0]=0;
        for(int i=1;i<n;i++)
            cur[i]=sum[i-1];
        for(int i=0;i<n;i++)
            if(sa[i]>0&&!tp[sa[i]-1])
                sa[cur[a[sa[i]-1]]++]=sa[i]-1;
        for(int i=0;i<n;i++)
            cur[i]=sum[i]-1;
        for(int i=n-1;i>=0;i--)
            if(sa[i]>0&&tp[sa[i]-1])
                sa[cur[a[sa[i]-1]]--]=sa[i]-1;
    }
    private static void sais(int[] a,int n) {
        boolean[] tp=new boolean [n+2];
        int[] p=new int [n+2];
        tp[n-1]=true;
        for(int i=n-2;i>=0;i--)
            tp[i]=(a[i]==a[i+1])?tp[i+1]:(a[i]<a[i+1]);
        int m=0;
        for(int i=0;i<n;i++)
            if(i>0&&tp[i]&&!tp[i-1]) {
                rk[i]=m;p[m++]=i;
            }
            else
                rk[i]=-1;
        induce(a,p,tp,n,m);
        int tot=0,x=0,y=0;
        int[] a1=new int [m];
        p[m]=n-1;
        for(int i=0;i<n;i++)
            if((x=rk[sa[i]])!=-1) {
                if(tot==0||p[x+1]-p[x]+1!=p[y+1]-p[y]+1)
                    tot++;
                else
                    for(int p1=p[x],p2=p[y];p2<=p[y+1];p1++,p2++)
                        if(a[p1]!=a[p2]||(a[p1]==a[p2]&&tp[p1]!=tp[p2])) {
                            tot++;
                            break;
                        }
                a1[y=x]=tot-1;
            }
        if(tot==m)
            for(int i=0;i<m;i++)
                sa[a1[i]]=i;
        else
            sais(a1,m);
        for(int i=0;i<m;i++)
            a1[i]=p[sa[i]];
        induce(a,a1,tp,n,m);
    }
    public static int[] solve(int[] s) {
        final int M=257;
        int[] cnt=new int [M];
        int n_o=s.length,n=n_o<<1;
        int[] tmp=new int [n];
        for(int i=0;i<n_o;i++)
            tmp[i]=tmp[n_o+i]=s[i];
        s=tmp;

        int[] a=new int [n+1];

        rk=new int [n+1];sa=new int [n+1];
        sum=new int [n+1];cur=new int [n+1];

        for(int i=0;i<M;i++)
            cnt[i]=0;
        for(int i=0;i<n;i++)
            cnt[s[i]]++;
        for(int i=1;i<M;i++)
            cnt[i]+=cnt[i-1];
        for(int i=0;i<n;i++)
            a[i]=cnt[s[i]];
        a[n]=0;
        sais(a,n+1);
        int[] ans=new int [n_o];
        int idx=0;
        for(int i=0;i<=n;i++)
            if(sa[i]<n_o)
                ans[idx++]=s[(sa[i]-1+n_o)%n_o];
        return ans;
    }
}

