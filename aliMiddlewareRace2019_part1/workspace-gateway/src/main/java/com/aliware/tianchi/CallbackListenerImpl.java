package com.aliware.tianchi;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.rpc.RpcStatus;
import org.apache.dubbo.rpc.listener.CallbackListener;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author daofeng.xjf
 *
 * 客户端监听器
 * 可选接口
 * 用户可以基于获取获取服务端的推送信息，与 CallbackService 搭配使用
 *
 */
public class CallbackListenerImpl implements CallbackListener {
    @Override
    public void receiveServerMsg(String msg) {
        try {
            System.out.println(new Date()+","+msg);
            String parts[] =  msg.split(",");
            int i = 2;
            if(parts[0].equals("small")){
                i = 0;
            }
            else if(parts[0].equals("medium")){
                i = 1;
            }
            UserLoadBalance.weight2[i] = Integer.parseInt(parts[1])*0.0001;
        }
        catch(Exception e){
            e.printStackTrace();
        }
    }

}