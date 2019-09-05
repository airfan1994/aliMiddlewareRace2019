package com.aliware.tianchi;

import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.remoting.exchange.Request;
import org.apache.dubbo.remoting.transport.RequestLimiter;
import org.apache.dubbo.rpc.RpcStatus;
import org.apache.dubbo.rpc.cluster.LoadBalance;
import org.apache.dubbo.rpc.service.CallbackService;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

/**
 * @author daofeng.xjf
 *
 * 服务端限流
 * 可选接口
 * 在提交给后端线程池之前的扩展，可以用于服务端控制拒绝请求
 */
public class TestRequestLimiter implements RequestLimiter {
    static volatile long inittime = System.currentTimeMillis();
    static volatile ArrayList<Long> list = new ArrayList<Long>();
    static volatile long limit = 0;
    static volatile long ts = System.currentTimeMillis();
    static volatile long periodacc = 0;
    static volatile long requestacc = 0;
    static volatile boolean shoulian = true;
    static volatile HashMap<Integer,Integer> map = new HashMap();
    static volatile ArrayList<Long> tz = new ArrayList();
    static volatile long silentts = 0;
    static volatile long converts = System.currentTimeMillis() + 1000000;
    static volatile long upper = 0;
    static volatile long lower = 0;
    static volatile int step = 5;
    /**
     * @param request 服务请求
     * @param activeTaskCount 服务端对应线程池的活跃线程数
     * @return  false 不提交给服务端业务线程池直接返回，客户端可以在 Filter 中捕获 RpcException
     *          true 不限流
     */
    @Override
    public boolean tryAcquire(Request request, int activeTaskCount) {

        long cur = System.currentTimeMillis();
        if(cur-ts>300) {
            synchronized (this.getClass()) {
                if (cur - ts > 300) {
                    ts = cur;
                    long periodcur = 0;
                    long requestscur = 0;
                    long avg = 0;

                    if (TestServerFilter.status != null) {
                        periodcur = TestServerFilter.status.getSucceededElapsed();
                        requestscur = TestServerFilter.status.getSucceeded();
                        if (requestscur - requestacc != 0) {
                            avg = (periodcur - periodacc) / (requestscur - requestacc);
                        }
                        list.add(avg);
                        if(!shoulian){
                                    tz.add(avg);
                                    System.out.println(new Date()+",limit:"+limit+",period:"+avg+",acc:"+(periodcur - periodacc));
                                    limit -= (upper-lower)/step;
                                    limit = Math.max(upper, limit);
                                    if(limit<lower){
//                                        long max = 0;
//                                        int mindex = 0;
//                                        for(int i=0;i<4;i++){
//                                            if(tz.get(i)-tz.get(i+1)>=max){
//                                                max = tz.get(i)-tz.get(i+1);
//                                                mindex = i;
//                                            }
//                                        }
//                                        if(max>=2&&mindex<4){
//                                            limit = upper-(upper-lower)/4*(mindex+1);
//                                            limit = (int)(limit*1.05);
//                                        }
//                                        else {
//                                            long max2 = 0;
//                                            int mindex2 = 1;
//                                            for(int i=1;i<=4;i++){
//                                                if(tz.get(i)-tz.get(i-1)>=max2){
//                                                    max2 = tz.get(i)-tz.get(i-1);
//                                                    mindex2 = i;
//                                                }
//                                            }
//                                            //if(max2>max&&max2>=2&&mindex2>0){
//                                            if(max2>=2&&mindex2>0){
//                                                limit = upper-(upper-lower)/4*(mindex2-1);
//                                                limit = (int)(limit*1.05);
//                                            }
//                                            else {
//                                                int i = 4;
//                                                for (; i > 0; i--) {
//                                                    if (tz.get(i) < tz.get(i - 1)) {
//                                                        break;
//                                                    }
//                                                }
//                                                limit = upper - (upper - lower) / 4 * (i-1);
//                                            }
                                        //}
                                        int  upindex = 0;
                                        int lowindex = 0;
                                        for(int i=1;i<=step;i++){
                                            if(tz.get(i)>tz.get(upindex)){
                                                upindex = i;
                                            }
                                            if(tz.get(i)<tz.get(lowindex)){
                                                lowindex = i;
                                            }
                                        }
                                        if(tz.get(upindex)-tz.get(lowindex)<=10){
//                                            if(upindex!=0){
//                                                limit = upper;
//                                            }
//                                            else{
//                                                int t = 1;
//                                                for(;t<=4;t++){
//                                                    if(tz.get(t)<tz.get(t-1)){
//                                                        break;
//                                                    }
//                                                }
//                                                limit = upper - (upper - lower) / 4 * (t-1);
//                                            }
                                            limit = upper;
                                        }
                                        else{
                                            limit = upper-(upper-lower)/step*(lowindex-1);
                                            limit = Math.min(limit, upper);
                                        }
                                        if(limit<upper){
                                            limit = (int)(limit*0.97);
                                        }
                                        shoulian = true;
                                        System.out.println(new Date() + ":complete converse:" + limit + "\n");
                                        silentts = cur;
                                    }
                        }
                        if (shoulian &&(valid3(list)||valid4(list))) {
                            shoulian = false;
                            limit = upper;
                            tz = new ArrayList<>();
                            silentts = 0;
                            converts = cur;
                            System.out.println(new Date()+":start converse");
                        }
                        requestacc = requestscur;
                        periodacc = periodcur;
                    }
                }
            }
        }
        limit = upper;
        if(TestServerFilter.status!=null&&limit!=0&&TestServerFilter.status.getActive()>=limit)
        {
            return false;
        }
        return true;
    }
    public static boolean valid3(ArrayList<Long> list){
        if(list.size()<6) return false;
        int index = list.size()-1;
        if(list.get(index)-list.get(index-2)>=2&&list.get(index)-list.get(index-3)>=2
                                &&list.get(index)-list.get(index-4)>=2
                                &&list.get(index)-list.get(index-5)>=2
                                &&list.get(index-1)-list.get(index-2)>=2
                                &&list.get(index-1)-list.get(index-3)>=2
                                &&list.get(index-1)-list.get(index-4)>=2
                                &&list.get(index-1)-list.get(index-5)>=2) {
            return true;
        }
        return false;
    }
    public static boolean valid4(ArrayList<Long> list){
        if(list.size()<6) return false;
        int index = list.size()-1;
        if(list.get(index-2)-list.get(index)>=2&&list.get(index-3)-list.get(index)>=2
                &&list.get(index-4)-list.get(index)>=2
                &&list.get(index-5)-list.get(index)>=2
                &&list.get(index-2)-list.get(index-1)>=2
                &&list.get(index-3)-list.get(index-1)>=2
                &&list.get(index-4)-list.get(index-1)>=2
                &&list.get(index-5)-list.get(index-1)>=2) {
            return true;
        }
        return false;
    }
}