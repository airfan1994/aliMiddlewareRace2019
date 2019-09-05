package com.aliware.tianchi;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.rpc.*;

import java.util.HashMap;
import java.util.Map;


/**
 * @author daofeng.xjf
 *
 * 服务端过滤器
 * 可选接口
 * 用户可以在服务端拦截请求和响应,捕获 rpc 调用时产生、服务端返回的已知异常。
 */
@Activate(group = Constants.PROVIDER)
public class TestServerFilter implements Filter {
    static volatile RpcStatus status = null;
    static volatile int max = 0;

    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        if(status==null) {
            String str = invoker.getUrl().toIdentityString();
            if(str.indexOf("Call")==-1){
                status = RpcStatus.getStatus(invoker.getUrl(), "hashservice");
                max = invoker.getUrl().getParameter(Constants.THREADS_KEY, Constants.DEFAULT_THREADS);
                TestRequestLimiter.upper = (int)(max*0.923);
                if (str.indexOf("small") != -1) {
                    TestRequestLimiter.upper = (int)(max*0.775);
                } else if (str.indexOf("medium") != -1) {
                    TestRequestLimiter.upper = (int)(max*0.9555);
                }

            }
        }
        long t = System.currentTimeMillis();
        boolean isSuccess = true;

        RpcStatus.beginCount(invoker.getUrl(), "hashservice");
        try{
            Result result = invoker.invoke(invocation);
            return result;
        }catch (Exception e){
            isSuccess = false;
            throw e;
        }
        finally {
            RpcStatus.endCount(invoker.getUrl(), "hashservice", 0, isSuccess);
        }
    }

    @Override
    public Result onResponse(Result result, Invoker<?> invoker, Invocation invocation) {
        return result;
    }

}
