package io.openmessaging;

import java.util.*;
import java.io.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class DemoTester2 {

    public static void main(String args[]) throws Exception {
        int checkTimes = 500;
        int sendTsNum = 12;
        int checkTsNum = 12;
        int maxMsgCheckSize = 80000;

        MessageStore messageStore = null;

        try {
            Class queueStoreClass = Class.forName("io.openmessaging.DefaultMessageStoreImpl");
            messageStore = (MessageStore)queueStoreClass.newInstance();
        } catch (Throwable t) {
            t.printStackTrace();
            System.exit(-1);
        }

        //Step1: 发送消息

        long[] as = new long[120000000];
        long[] ts = new long[120000000];
        BufferedReader br = new BufferedReader(new FileReader("/Users/airfan/workspace/tianchi/data2/data.txt"));
        String line = br.readLine();
        int t = 0;
        while(line!=null){
            String[] parts = line.split(",");
            ts[t] = Long.parseLong(parts[0]);
            as[t] = Long.parseLong(parts[1]);
            line = br.readLine();
            t++;
        }
        br.close();

        Thread[] sends = new Thread[sendTsNum];
        for (int i = 0; i < sendTsNum; i++) {
            sends[i] = new Thread(new Producer(messageStore, as, ts));
        }
        for (int i = 0; i < sendTsNum; i++) {
            sends[i].start();
        }
        for (int i = 0; i < sendTsNum; i++) {
            sends[i].join();
        }

        //Step2: 查询聚合消息
//        System.out.println("start check");
//        Thread[] msgChecks = new Thread[checkTsNum];
//        for (int i = 0; i < checkTsNum; i++) {
//            msgChecks[i] = new Thread(new MessageChecker(messageStore, checkTimes, maxMsgCheckSize, as, ts));
//        }
//        for (int i = 0; i < checkTsNum; i++) {
//            msgChecks[i].start();
//        }
//        for (int i = 0; i < checkTsNum; i++) {
//            msgChecks[i].join();
//        }

        //Step3: 查询聚合消息
        Thread[] checks = new Thread[checkTsNum];
        for (int i = 0; i < checkTsNum; i++) {
            checks[i] = new Thread(new ValueChecker(messageStore, checkTimes, maxMsgCheckSize, as, ts));
        }
        for (int i = 0; i < checkTsNum; i++) {
            checks[i].start();
        }
        for (int i = 0; i < checkTsNum; i++) {
            checks[i].join();
        }
    }
    static class Producer implements Runnable {

        private MessageStore messageStore;
        long[] as;
        long[] ts;
        ThreadLocal<Integer> pid = new ThreadLocal<>();
        private static volatile AtomicInteger pcounter = new AtomicInteger();
        public Producer(MessageStore messageStore, long[] as, long[] ts) {
            this.messageStore = messageStore;
            this.as = as;
            this.ts = ts;
        }

        @Override
        public void run() {
            pid.set(pcounter.getAndIncrement());
            for(int i=pid.get();i<as.length;i+=12){
                messageStore.put(new Message(as[i], ts[i], new byte[34]));
            }
        }
    }

    static class MessageChecker implements Runnable {

        private MessageStore messageStore;
        private int checkTimes;
        private int maxCheckSize;
        long[] as;
        long[] ts;

        public MessageChecker(MessageStore messageStore,  int checkTimes, int maxCheckSize, long[] as, long[] ts) {
            this.checkTimes = checkTimes;
            this.messageStore = messageStore;
            this.maxCheckSize = maxCheckSize;
            this.ts = ts;
            this.as = as;
        }

        private void checkError() {
            System.out.println("message check error");
            System.exit(-1);
        }

        @Override
        public void run() {
            for(int i=0;i<checkTimes;i++){
                long aMin = 0;
                long aMax = 40000000000L;
//                int startindex = (int)(120000000*Math.random());
//                int endindex = Math.min(120000000-1, startindex+(int)(maxCheckSize*Math.random()));
                int startindex = 120000000-(int)(maxCheckSize*Math.random());
                int endindex = 120000000-1;
                long tMin = ts[startindex];
                long tMax = ts[endindex];
                List<Message> list = messageStore.getMessage(aMin, aMax, tMin, tMax);
                for(int t=0;t<list.size();t++){
                    Message m = list.get(i);
                    if(m.getT()!=ts[t+startindex]||m.getA()!=as[t+startindex]){
                        checkError();
                    }
                }
            }
        }
    }


    static class ValueChecker implements Runnable {

        private MessageStore messageStore;
        private int checkTimes;
        private int maxCheckSize;
        long[] as;
        long[] ts;

        public ValueChecker(MessageStore messageStore, int checkTimes, int maxCheckSize, long[] as, long[] ts) {
            this.checkTimes = checkTimes;
            this.messageStore = messageStore;
            this.maxCheckSize = maxCheckSize;
            this.as = as;
            this.ts = ts;
        }

        private void checkError(long aMin, long aMax, long tMin, long tMax, long res, long val) {
            System.out.printf("value check error. aMin:%d, aMax:%d, tMin:%d, tMax:%d, res:%d, val:%d\n",
                    aMin, aMax, tMin, tMax, res, val);
            System.exit(-1);
        }

        @Override
        public void run() {
            for(int i=0;i<checkTimes;i++){
                long aMin = 0;
                long aMax = 3000000000000L;
                int startindex = (int)(120000000*Math.random());
                int endindex = Math.min(120000000-1, startindex+(int)(maxCheckSize*Math.random()));
//                int startindex = 120000000-(int)(maxCheckSize*Math.random());
//                int endindex = 120000000-1;
                long tMin = ts[startindex];
                long tMax = ts[endindex];
                long val = messageStore.getAvgValue(aMin, aMax, tMin, tMax);
                long sumres = 0;
                long countres = 0;
                for(int t=startindex;t<=endindex;t++){
                    if(as[t]<=aMax) {
                        sumres += as[t];
                        countres++;
                    }
                }
                System.out.println(i+" avg:"+sumres+","+countres);
                long res = sumres/countres;
                if(res!=val){
                    checkError(aMin,aMax,tMin,tMax,res,val);
                }
            }
        }
    }
}