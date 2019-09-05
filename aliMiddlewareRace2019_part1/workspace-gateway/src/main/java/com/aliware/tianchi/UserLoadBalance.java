package com.aliware.tianchi;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.RpcStatus;
import org.apache.dubbo.rpc.cluster.LoadBalance;
import org.omg.IOP.TAG_CODE_SETS;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import javax.swing.text.html.HTMLDocument.HTMLReader.PreAction;

/**
 * @author daofeng.xjf
 *
 * 负载均衡扩展接口
 * 必选接口，核心接口
 * 此类可以修改实现，不可以移动类或者修改包名
 * 选手需要基于此类实现自己的负载均衡算法
 */
public class UserLoadBalance implements LoadBalance {
    static volatile RpcStatus[] statuses = new RpcStatus[3];
    static volatile double[] weight3 = new double[]{0.155,0.43,0.6};
    static volatile double[] weight2 = new double[]{0.0,0.0,0.0};
    @Override
    public <T> Invoker<T> select(List<Invoker<T>> invokers, URL url, Invocation invocation) throws RpcException {

        if(statuses[0]==null){
            statuses[0] = RpcStatus.getStatus(invokers.get(0).getUrl(),invocation.getMethodName());
            statuses[1] = RpcStatus.getStatus(invokers.get(1).getUrl(),invocation.getMethodName());
            statuses[2] = RpcStatus.getStatus(invokers.get(2).getUrl(),invocation.getMethodName());
        }
        if(invokers.size()<3){
            double sum = 0.0;
            for(int i=0;i<invokers.size();i++){
                String str = invokers.get(i).getUrl().toIdentityString();
                int n = 2;
                if (str.indexOf("small") != -1) {
                    n = 0;
                } else if (str.indexOf("medium") != -1) {
                    n = 1;
                }
                sum+=weight3[n];
            }
            double ran = ThreadLocalRandom.current().nextDouble(sum);
            double tmp = 0.0;
            int i=0;
            for(;i<invokers.size();i++){
                String str = invokers.get(i).getUrl().toIdentityString();
                int n = 2;
                if (str.indexOf("small") != -1) {
                    n = 0;
                } else if (str.indexOf("medium") != -1) {
                    n = 1;
                }
                tmp+=weight3[n];
                if(tmp>=ran){
                    break;
                }
            }
            RpcStatus.beginCount(invokers.get(i).getUrl(), invocation.getMethodName(), 0);
            return invokers.get(i);
        }
            int i = invokers.size() - 1;
            double min = Double.MAX_VALUE;
            for (int t = invokers.size() - 1; t >= 0; t--) {
                double score = statuses[t].getActive() * 1.0 / (weight3[t]+weight2[i]);
                if (score < min) {
                    min = score;
                    i = t;
                }
            }
        RpcStatus.beginCount(invokers.get(i).getUrl(), invocation.getMethodName(), 0);
        return invokers.get(i);
    }
}