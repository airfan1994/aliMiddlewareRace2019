package io.openmessaging;

import java.nio.ByteBuffer;
import java.util.*;

import java.util.concurrent.atomic.AtomicLong;


//这是评测程序的一个demo版本，其评测逻辑与实际评测程序基本类似，但是比实际评测简单很多
//该评测程序主要便于选手在本地优化和调试自己的程序

public class DemoTester {

    public static void main(String args[]) throws Exception {
        //评测相关配置
        //发送阶段的发送数量，也即发送阶段必须要在规定时间内把这些消息发送完毕方可
        int msgNum  = 700000000;
        //int msgNum  = 10000000;
        //发送阶段的最大持续时间，也即在该时间内，如果消息依然没有发送完毕，则退出评测
        int sendTime = 10 * 60 * 1000;
        //查询阶段的最大持续时间，也即在该时间内，如果消息依然没有消费完毕，则退出评测
        int checkTime = 10 * 60 * 1000;

        //正确性检测的次数
        int checkTimes = 50;
        //发送的线程数量
        int sendTsNum = 12;
        //查询的线程数量
        int checkTsNum = 12;
        // 每次查询消息的最大跨度
        int maxMsgCheckSize = 80000;
        // 每次查询求平均的最大跨度
        int maxValueCheckSize = 80000;

        MessageStore messageStore = null;

        try {
            Class queueStoreClass = Class.forName("io.openmessaging.DefaultMessageStoreImpl");
            messageStore = (MessageStore)queueStoreClass.newInstance();
        } catch (Throwable t) {
            t.printStackTrace();
            System.exit(-1);
        }

        //Step1: 发送消息
        long sendStart = System.currentTimeMillis();
        long maxTimeStamp = System.currentTimeMillis() + sendTime;
        AtomicLong sendCounter = new AtomicLong(0);
        Thread[] sends = new Thread[sendTsNum];
        for (int i = 0; i < sendTsNum; i++) {
            sends[i] = new Thread(new Producer(messageStore, maxTimeStamp, msgNum, sendCounter));
        }
        for (int i = 0; i < sendTsNum; i++) {
            sends[i].start();
        }
        for (int i = 0; i < sendTsNum; i++) {
            sends[i].join();
        }
        long sendSend = System.currentTimeMillis();
        System.out.printf("Send: %d ms Num:%d\n", sendSend - sendStart, msgNum);
        long maxCheckTime = System.currentTimeMillis() + checkTime;

        //Step2: 查询聚合消息
        long msgCheckStart = System.currentTimeMillis();
        AtomicLong msgCheckTimes = new AtomicLong(0);
        AtomicLong msgCheckNum = new AtomicLong(0);
        Thread[] msgChecks = new Thread[checkTsNum];
        for (int i = 0; i < checkTsNum; i++) {
            msgChecks[i] = new Thread(new MessageChecker(messageStore, maxCheckTime, checkTimes, msgNum, maxMsgCheckSize, msgCheckTimes, msgCheckNum));
        }
        for (int i = 0; i < checkTsNum; i++) {
            msgChecks[i].start();
        }
        for (int i = 0; i < checkTsNum; i++) {
            msgChecks[i].join();
        }
        long msgCheckEnd = System.currentTimeMillis();
        System.out.printf("Message Check: %d ms Num:%d\n", msgCheckEnd - msgCheckStart, msgCheckNum.get());

        //Step3: 查询聚合结果
        long checkStart = System.currentTimeMillis();
        AtomicLong valueCheckTimes = new AtomicLong(0);
        AtomicLong valueCheckNum = new AtomicLong(0);
        Thread[] checks = new Thread[checkTsNum];
        for (int i = 0; i < checkTsNum; i++) {
            checks[i] = new Thread(new ValueChecker(messageStore, maxCheckTime, checkTimes, msgNum, maxValueCheckSize, valueCheckTimes, valueCheckNum));
        }
        for (int i = 0; i < checkTsNum; i++) {
            checks[i].start();
        }
        for (int i = 0; i < checkTsNum; i++) {
            checks[i].join();
        }
        long checkEnd = System.currentTimeMillis();
        System.out.printf("Value Check: %d ms Num: %d\n", checkEnd - checkStart, valueCheckNum.get());

        //评测结果
        //System.out.printf("Total Score:%d\n", (msgNum / (sendSend- sendStart) + msgCheckNum.get() / (msgCheckEnd - msgCheckStart) + valueCheckNum.get() / (msgCheckEnd - msgCheckStart)));
        //System.out.printf("Total Score:%d\n", (msgNum / (sendSend- sendStart) + msgCheckNum.get() / (msgCheckEnd - msgCheckStart) + valueCheckNum.get() / (checkEnd - checkStart)));
    }
    static class Producer implements Runnable {

        private AtomicLong counter;
        private long maxMsgNum;
        private MessageStore messageStore;
        private long maxTimeStamp;
        public Producer(MessageStore messageStore, long maxTimeStamp, int maxMsgNum, AtomicLong counter) {
            this.counter = counter;
            this.maxMsgNum = maxMsgNum;
            this.messageStore = messageStore;
            this.maxTimeStamp =  maxTimeStamp;
        }

        @Override
        public void run() {
            long count;
            while ( (count = counter.getAndIncrement()) < maxMsgNum && System.currentTimeMillis() <= maxTimeStamp) {
                try {
//                    ByteBuffer buffer = ByteBuffer.allocate(34);
//                    buffer.putLong(0, count);
                    // 为测试方便, 插入的是有规律的数据, 不是实际测评的情况
                   // messageStore.put(new Message(count, count, buffer.array()));
//                    if(count%10==1){
//                        messageStore.put(new Message(count+50000, count, getBody((int)count)));
//                    }
//                    else if(count%10==6){
//                        messageStore.put(new Message(count-3, count, getBody((int)count)));
//                    }
//                    else {
//                        messageStore.put(new Message(count, count, getBody((int) count)));
//                    }
                    messageStore.put(new Message(count, count, getBody((int) count)));
//                    if ((count & 0x1L) == 0) {
//                        //偶数count多加一条消息
//                        messageStore.put(new Message(count, count, buffer.array()));
//                    }
                } catch (Throwable t) {
                    t.printStackTrace();
                    System.exit(-1);
                }
            }
        }
        public static byte[] getBody(int count){
            byte[] b = new byte[34];
            b[0] = (byte)(count&0xff);
            b[1] = (byte)((count>>8)&0xff);
            b[2] = (byte)((count>>16)&0xff);
            b[3] = (byte)((count>>24)&0xff);
            return b;
        }
    }

    static class MessageChecker implements Runnable {

        private AtomicLong timesCounter;
        private AtomicLong numCounter;
        private long checkTimes;
        private MessageStore messageStore;
        private long maxTimeStamp;
        private int maxIndex;
        private int maxCheckSize;

        public MessageChecker(MessageStore messageStore, long maxTimeStamp, int checkTimes, int maxIndex, int maxCheckSize,
                              AtomicLong timesCounter, AtomicLong numCounter) {
            this.timesCounter = timesCounter;
            this.numCounter = numCounter;
            this.checkTimes = checkTimes;
            this.messageStore = messageStore;
            this.maxTimeStamp =  maxTimeStamp;
            this.maxIndex = maxIndex;
            this.maxCheckSize = maxCheckSize;
        }

        private void checkError() {
            System.out.println("message check error");
            System.exit(-1);
        }

        @Override
        public void run() {
            Random random = new Random();
            while (timesCounter.getAndIncrement() < checkTimes && System.currentTimeMillis() <= maxTimeStamp) {
                try {
                    int aIndex1 = random.nextInt(maxIndex - 1);
                    if (aIndex1 < 0) {
                        aIndex1 = 0;
                    }
                    int aIndex2 = Math.min(aIndex1 + maxCheckSize, maxIndex - 1);

                    int tIndex1 = random.nextInt(aIndex2 - aIndex1) + aIndex1;
                    if (tIndex1 < 0) {
                        tIndex1 = 0;
                    }
                    int tIndex2 = random.nextInt(maxCheckSize) + tIndex1;

//                    int tIndex1 = 993087;
//                    int tIndex2 = 1003137;
//                    int aIndex1 = 991680;
//                    int aIndex2 = 999999;
                    int index1 = Math.max(aIndex1, tIndex1);
                    int index2 = Math.min(aIndex2, tIndex2);

                    List<Message> msgs = messageStore.getMessage(aIndex1, aIndex2, tIndex1, tIndex2);

                    //验证消息
                    //System.out.println("sds1");
                    Iterator<Message> iter = msgs.iterator();
                    //System.out.println("sds2");
                    while (iter.hasNext()) {
                        if (index1 > index2) {
                      //      System.out.println("sds");
                            checkError();
                        }

                        Message msg = iter.next();
//                        if (msg.getA() != msg.getT() || msg.getA() != index1 ||
//                                ByteBuffer.wrap(msg.getBody()).getLong() != index1) {
//                            checkError();
//                        }
//                        long a = 0;
//                        if(index1%10==1){
//                            a=index1+50000;
//                        }
//                        else if(index1%10==6){
//                            a=index1-3;
//                        }
//                        else {
//                            a= index1;
//                        }
//                        if(index1%5==1){
//                            ++index1;
//                            continue;
//                        }
                        //System.out.println(msg.getT());
                        if (msg.getA() != msg.getT() || msg.getA() != index1 || !new String(msg.getBody()).equals(new String(getBody((int)msg.getA())))) {
                            System.out.println(tIndex1+","+tIndex2+","+aIndex1+","+aIndex2);
                            System.out.println(msg.getA()+","+msg.getT());
                            System.out.println(msg.getA()+","+index1);
                            System.out.println(new String(msg.getBody()).equals(new String(getBody((int)msg.getA()))));
                            checkError();
                        }

                        //偶数需要多验证一次
//                        if ((index1 & 0x1) == 0 && iter.hasNext()) {
//                            msg = iter.next();
//                            if (msg.getA() != msg.getT() || msg.getA() != index1
//                                    || ByteBuffer.wrap(msg.getBody()).getLong() != index1) {
//                                checkError();
//                            }
//                        }

                        ++index1;
                    }


                    if (index1 - 1 != index2) {
                        checkError();
                    }

                    numCounter.getAndAdd(msgs.size());
                } catch (Throwable t) {
                    t.printStackTrace();
                    System.exit(-1);

                }
            }
        }
        public static byte[] getBody(int count){
            byte[] b = new byte[34];
            b[0] = (byte)(count&0xff);
            b[1] = (byte)((count>>8)&0xff);
            b[2] = (byte)((count>>16)&0xff);
            b[3] = (byte)((count>>24)&0xff);
            return b;
        }
    }

    static class ValueChecker implements Runnable {

        private AtomicLong timesCounter;
        private AtomicLong numCounter;
        private long checkTimes;
        private MessageStore messageStore;
        private long maxTimeStamp;
        private int maxIndex;
        private int maxCheckSize;

        public ValueChecker(MessageStore messageStore, long maxTimeStamp, int checkTimes, int maxIndex, int maxCheckSize,
                            AtomicLong timesCounter, AtomicLong numCounter) {
            this.timesCounter = timesCounter;
            this.numCounter = numCounter;
            this.checkTimes = checkTimes;
            this.messageStore = messageStore;
            this.maxTimeStamp =  maxTimeStamp;
            this.maxIndex = maxIndex;
            this.maxCheckSize = maxCheckSize;
        }

        private void checkError(long aMin, long aMax, long tMin, long tMax, long res, long val) {
            System.out.printf("value check error. aMin:%d, aMax:%d, tMin:%d, tMax:%d, res:%d, val:%d\n",
                    aMin, aMax, tMin, tMax, res, val);
            System.exit(-1);
        }

        @Override
        public void run() {
            Random random = new Random();
            while (timesCounter.getAndIncrement() < checkTimes && System.currentTimeMillis() <= maxTimeStamp) {
                try {
                    int aIndex1 = random.nextInt(maxIndex - 1);
                    if (aIndex1 < 0) {
                        aIndex1 = 0;
                    }
                    int aIndex2 = Math.min(aIndex1 + maxCheckSize, maxIndex - 1);

                    int tIndex1 = random.nextInt(aIndex2 - aIndex1) + aIndex1;
                    if (tIndex1 < 0) {
                        tIndex1 = 0;
                    }
                    int tIndex2 = random.nextInt(maxCheckSize) + tIndex1;
                    int index1 = Math.max(aIndex1, tIndex1);
                    int index2 = Math.min(aIndex2, tIndex2);
//                    int aIndex1 = 483602;
//                    int aIndex2 = 483603;
//                    int tIndex1 = 483602;
//                    int tIndex2 = 483603;
//                    int index1 = 483602;
//                    int index2 = 483603;


                    long val = messageStore.getAvgValue(aIndex1, aIndex2, tIndex1, tIndex2);

                    //验证
//                    int evenIndex1 = (index1 & 0x1) == 0 ? index1 : index1 + 1;
//                    int evenIndex2 = (index2 & 0x1) == 0 ? index2 : index2 - 1;
//
//                    long res = 0;
//                    long count = 0;
//                    if (evenIndex1 <= evenIndex2) {
//                        //顺序数之和
//                        long sum1 = ((long)(index2 + index1) * (index2 - index1 + 1)) >>> 1;
//                        //重复的偶数之和
////                        long sum2 = ((long)(evenIndex1 + evenIndex2) * ((evenIndex2 - evenIndex1 >>> 1) + 1)) >>> 1;
////                        long sum = sum1 + sum2;
////                        count = index2 - index1 + 1 + (evenIndex2 - evenIndex1 >>> 1) + 1;
//                        count = index2 - index1 + 1;
//                        res = sum1 / count;
//                    } else {
//                        //顺序数之和
//                        long sum = (index2 + index1) * (index2 - index1 + 1) >>> 1;
//                        count = index2 - index1 + 1;
//                        res = sum / count;
//                    }


//                    if(count%10==1){
//                        messageStore.put(new Message(count+50000, count, getBody((int)count)));
//                    }
//                    else if(count%10==6){
//                        messageStore.put(new Message(count-3, count, getBody((int)count)));
//                    }
//                    else {
//                        messageStore.put(new Message(count, count, getBody((int) count)));
//                    }


                    long sum = 0;
                    long count = 0;
                    for(long t=tIndex1;t<=tIndex2;t++){
//                        long a = 0;
//                        if(t%10==1){
//                            a=t+50000;
//                        }
//                        else if(t%10==6){
//                            a=t-3;
//                        }
//                        else {
//                            a= t;
//                        }
                        long a=t;
                        if(t>=tIndex1&&a>=aIndex1&&t<=tIndex2&&a<=aIndex2) {
                            sum+=a;
                            count++;
                        }
                    }
                    System.out.println("sum:"+count+","+sum+","+tIndex2);
                    long res = sum/count;

                    if (res != val) {
                        checkError(aIndex1, aIndex2, tIndex1, tIndex2, res, val);
                    }

                    numCounter.getAndAdd(count);
                } catch (Throwable t) {
                    t.printStackTrace();
                    System.exit(-1);

                }
            }
        }
    }
}