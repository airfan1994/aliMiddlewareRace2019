package io.openmessaging;

import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.io.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 这是一个简单的基于内存的实现，以方便选手理解题意；
 * 实际提交时，请维持包名和类名不变，把方法实现修改为自己的内容；
 */
public class DefaultMessageStoreImpl extends MessageStore {

    //private NavigableMap<Long, List<Message>> msgMap = new TreeMap<Long, List<Message>>();
//    static volatile AtomicLong counter = new AtomicLong(0);
    private static final int nthreads = 12;
    public static final int msglen1 = 8;
    public static final int msglen2 = 34;
    public static final int msglen3 = 40;
    public static final int nsmgsinglebatch = 256;
    private static final int singlebatchlen1 = msglen1 * nsmgsinglebatch;
 //   private static final int singlebatchlen2 = msglen2 * nsmgsinglebatch;
 //   private static final int num = 200000000/nsmgsinglebatch;
    public static final int nmsg = 4096*4;
    private static final int nbatch = nmsg/nsmgsinglebatch;
    //private static volatile long[][] tmax = new long[nthreads][num];

    //for put and write thread
    private static volatile RandomAccessFile[] raf = new RandomAccessFile[nthreads];
    public static volatile FileChannel[] fc = new FileChannel[nthreads];
    public static volatile ByteBuffer[] data = new ByteBuffer[nthreads];
    private static final int nwthreads = 3;
    private static volatile WTHread[] wts = new WTHread[nthreads];
    public static volatile ConcurrentLinkedQueue<wByteBuffer2>[] toput = new ConcurrentLinkedQueue[nthreads];
    public static volatile ConcurrentLinkedQueue<wByteBuffer2>[] todump = new ConcurrentLinkedQueue[nthreads];

    //for merge thread
    public static volatile MergeThread mth;
    public static final int nparitions = 10;
    private static volatile RandomAccessFile[] rafta2 = new RandomAccessFile[nparitions];
    public static volatile FileChannel[] fcta2 = new FileChannel[nparitions];
    private static volatile RandomAccessFile[] rafbody2 = new RandomAccessFile[nparitions];
    public static volatile FileChannel[] fcbody2 = new FileChannel[nparitions];
    public static volatile ConcurrentLinkedQueue<wByteBuffer>[] toput2 = new ConcurrentLinkedQueue[nthreads];
    public static volatile ConcurrentLinkedQueue<wByteBuffer>[] todump2 = new ConcurrentLinkedQueue[nthreads];
    public static ByteBuffer[] buffermerges = new ByteBuffer[nthreads];
    public static volatile FileChannel[] fcm = new FileChannel[nthreads];

    private static ThreadLocal<Integer> index = new ThreadLocal<>();
    private static volatile AtomicInteger putcounter = new AtomicInteger(0);
    public static volatile int[] tmpcount = new int[nthreads];
    //private static volatile int[] count = new int[nthreads];

    private static volatile String path = "/alidata1/race2019/data/";
    //private static volatile String path = "/Users/airfan/workspace/tianchi/data/";
    private static final int fetchsize = 100000;
    private static ByteBuffer[] fetchbuffertas = new ByteBuffer[nthreads];
    private static ByteBuffer[] fetchbufferbodys = new ByteBuffer[nthreads];
    ThreadLocal<Integer> fetchtid = new ThreadLocal<>();
    private static volatile AtomicInteger fetchtcounter = new AtomicInteger(0);
    private static volatile ByteBuffer[] avgbuffers = new ByteBuffer[nthreads];
    ThreadLocal<Integer> avgtid = new ThreadLocal<>();
    private static volatile AtomicInteger avgtcounter = new AtomicInteger(0);
    public static volatile int[] putcount = new int[nthreads];
    public static volatile long[][] tmax = new long[nparitions][2136000000/nparitions/nsmgsinglebatch];
    public static volatile long[][] asum = new long[nparitions][2136000000/nparitions/nsmgsinglebatch];

    //for cache
    public static int cachesize = 178500000;
    public static int cacheparttsize = 5000000;
    public static int cachetend = 255;
    public static int cachepsize2 = 1000;
    public static volatile byte[] cache = new byte[cachesize*nthreads];
    public static volatile int[] cacheparttkey = new int[nthreads*cacheparttsize];
    public static volatile long[] cacheparttval = new long[nthreads*cacheparttsize];
    public static volatile int[] cacheparttcount = new int[nthreads];
    public static volatile int[] cachekey2 = new int[nthreads*cachepsize2];
    public static volatile long[] cacheval2 = new long[nthreads*cachepsize2];
    public static volatile int[] cachecount2 = new int[nthreads];
    public static volatile int[] cachekey3 = new int[nthreads*cachepsize2];
    public static volatile long[] cacheval3 = new long[nthreads*cachepsize2];
    public static volatile int cachecount3 = 0;

    public static ThreadLocal<Long> pre = new ThreadLocal<>();
    public static volatile int[] total = new int[nthreads];
    public static int[] apcount = new int[nthreads];
//    public static volatile long[] stepput = new long[nthreads];
//    public static volatile long[] stepfetch = new long[nparitions];

    static{
        for(int i=0;i<nthreads;i++){
            try{
                raf[i] = new RandomAccessFile(path + i + ".txt", "rw");
                fc[i] = raf[i].getChannel();
                toput[i] = new ConcurrentLinkedQueue<>();
                todump[i] = new ConcurrentLinkedQueue<>();
                toput2[i] = new ConcurrentLinkedQueue<>();
                todump2[i] = new ConcurrentLinkedQueue<>();
                for(int t=0;t<160;t++){
                    toput[i].offer(new wByteBuffer2(ByteBuffer.allocateDirect(msglen3 * nmsg),0));
                }
                for(int t=0;t<80;t++){
                    toput[i].offer(new wByteBuffer2(ByteBuffer.allocate(msglen3 * nmsg),0));
                }
                data[i] = ByteBuffer.allocateDirect(nmsg*msglen3);
            }
            catch(Exception e){
                e.printStackTrace();
            }
        }
        try {
            for(int i=0;i<nparitions;i++) {
                rafta2[i] = new RandomAccessFile(path + i +"ta2.txt", "rw");
                fcta2[i] = rafta2[i].getChannel();
                rafbody2[i] = new RandomAccessFile(path + i+ "body2.txt", "rw");
                fcbody2[i] = rafbody2[i].getChannel();
            }
        }
        catch(Exception e){
            e.printStackTrace();
        }

        //for wthread
        WTHread.todump = todump;
        WTHread.toput = toput;
        WTHread.fc = fc;
        for(int i=0;i<nwthreads;i++){
            wts[i] = new WTHread(i, 4);
        }
        for(int i=0;i<nwthreads;i++){
            wts[i].start();
        }

        //for fetch
        for(int i=0;i<fetchbuffertas.length;i++){
            fetchbuffertas[i] = ByteBuffer.allocateDirect(msglen1 * fetchsize);
            fetchbufferbodys[i] = ByteBuffer.allocateDirect(msglen2 * fetchsize);
        }

        //for avg
        for(int i=0;i<avgbuffers.length;i++){
            avgbuffers[i] = ByteBuffer.allocateDirect(8000*singlebatchlen1);
        }
    }

    @Override
    public void put(Message message) {
        if(index.get()==null){
            index.set(putcounter.getAndIncrement());
        }

        int pindex = index.get();
        if (pre.get() != null) {
            int tstep = (int)(message.getT() - pre.get());
            if(tstep<=cachetend){
                cachemsg((byte)(tstep-128), cache, total[pindex] + pindex*cachesize);
            }
            else{
                cacheparttkey[pindex*cacheparttsize + cacheparttcount[pindex]] = total[pindex];
                cacheparttval[pindex*cacheparttsize + cacheparttcount[pindex]++] = message.getT();
                if(pindex*cacheparttsize + cacheparttcount[pindex]==cacheparttval.length){
                    System.out.println("rehashing");
                    int[] cacheparttkey2 = new int[nthreads*cacheparttsize*2];
                    long[] cacheparttval2 = new long[nthreads*cacheparttsize*2];
                    System.arraycopy(cacheparttkey, 0, cacheparttkey2, 0, cacheparttsize*nthreads);
                    System.arraycopy(cacheparttval, 0, cacheparttval2, 0, cacheparttsize*nthreads);
                    cacheparttsize*=2;
                    DefaultMessageStoreImpl.cacheparttkey = cacheparttkey2;
                    DefaultMessageStoreImpl.cacheparttval = cacheparttval2;
                }
            }
        }
        else{
            cacheparttkey[pindex*cacheparttsize + cacheparttcount[pindex]] = total[pindex];
            cacheparttval[pindex*cacheparttsize + cacheparttcount[pindex]++] = message.getT();
            if(pindex*cacheparttsize + cacheparttcount[pindex]==cacheparttval.length){
                System.out.println("rehashing2");
                int[] cacheparttkey2 = new int[nthreads*cacheparttsize*2];
                long[] cacheparttval2 = new long[nthreads*cacheparttsize*2];
                System.arraycopy(cacheparttkey, 0, cacheparttkey2, 0, cacheparttsize*nthreads);
                System.arraycopy(cacheparttval, 0, cacheparttval2, 0, cacheparttsize*nthreads);
                cacheparttsize*=2;
                DefaultMessageStoreImpl.cacheparttkey = cacheparttkey2;
                DefaultMessageStoreImpl.cacheparttval = cacheparttval2;
            }
        }

        pre.set(message.getT());
        enmsg(message, data[pindex], pindex);
        total[pindex]++;
        tmpcount[pindex]++;
        if(tmpcount[pindex]==nmsg){
            putcount[pindex]++;
            todump[pindex].offer(new wByteBuffer2(data[pindex],0));
            while(toput[pindex].isEmpty()){
//                try{
//                    Thread.sleep(0, 1);
//                }
//                catch(Exception e){
//                    e.printStackTrace();
//                }
            }
            wByteBuffer2 bbs = toput[pindex].poll();
            data[index.get()] = bbs.buffer;
            tmpcount[index.get()] = 0;
        }

    }


    @Override
    public List<Message> getMessage(long aMin, long aMax, long tMin, long tMax) {
        System.out.println(new Date()+":"+aMin+","+aMax+","+tMin+","+tMax+","+Thread.currentThread().getName());
        ArrayList<Message> res = new ArrayList<Message>();
        if(fetchtid.get()==null){
            fetchtid.set(fetchtcounter.getAndIncrement());
//            System.out.println(new Date()+":"+Thread.currentThread().getName()+",start put");
            if(fetchtid.get()==0){
                WTHread.putdone = true;
                while(true) {
                                     int h = 0;
                                     for(;h<nwthreads;h++){
                                             if(!wts[h].done){
                                                     break;
                                                 }
                                         }
                                     if(h==nwthreads){
                                             break;
                                         }
                                     else{
                                             try {
                                                     Thread.sleep(1);
                                                 }
                                             catch(Exception e){
                                                     e.printStackTrace();
                                                 }
                                         }
                }
                //store t into disk

                for(int i=0;i<nthreads;i++){
                    buffermerges[i] = ByteBuffer.allocateDirect(nmsg*msglen1);
                    try {
                        fcm[i] = new RandomAccessFile(path + i + "t.txt", "rw").getChannel();
                        int pos = 0;
                        long t = cacheparttval[i*cacheparttsize];

                        //System.out.println("t0:"+t);
                        for(int h = 0;h < putcount[i];h++){
                            for(int m = 0;m < nmsg;m++){
                                int kt = getkey3(cacheparttkey, i*cacheparttsize, i*cacheparttsize+cacheparttcount[i], pos);
                                if(kt!=-1){
                                    if(pos!=0) {
                                        t = cacheparttval[kt];
                                    }
                                }
                                else {
                                    t = t + cache[i*cachesize + pos] + 128;
                                }
//                                if(pos<100)
//                                    System.out.println("put:"+i+","+t);
                                pos++;
                                buffermerges[i].putLong(t);
                            }
                            buffermerges[i].position(0);
                            fcm[i].write(buffermerges[i]);
                            buffermerges[i].position(0);
                        }
                        if(tmpcount[i]>0){
                            for(int m = 0;m < tmpcount[i];m++){
                                int kt = getkey3(cacheparttkey, i*cacheparttsize, i*cacheparttsize+cacheparttcount[i], pos);
                                if(kt!=-1){
                                    t = cacheparttval[kt];
                                }
                                else {
                                    t = t + cache[i*cachesize + pos] + 128;
                                }
                                pos++;
                                buffermerges[i].putLong(t);
                            }
                            buffermerges[i].position(0);
                            fcm[i].write(buffermerges[i]);
                            buffermerges[i].position(0);
                        }
                    }
                    catch(Exception e){
                        e.printStackTrace();
                    }
                }
                Arrays.fill(cache, (byte)0);
                Arrays.fill(cacheparttkey, 0);
                Arrays.fill(cacheparttval, 0L);
                Arrays.fill(cacheparttcount ,0);

                for(int i=0;i<nthreads;i++){
                    while(!toput[i].isEmpty()){
                        toput2[i].offer(new wByteBuffer(ByteBuffer.allocateDirect(nmsg*msglen1), toput[i].poll().buffer, 0));
                    }
                }
                mth = new MergeThread(nmsg, toput2, todump2, fcta2, fcbody2);
                mth.start();
                PutThread[] pths = new PutThread[nthreads];
                for(int i=0;i<nthreads;i++){
                    pths[i] = new PutThread(i);
                }
                for(int i=0;i<nthreads;i++){
                    pths[i].start();
                }
            }

            while(!MergeThread.dumpdone){
                try {
                    Thread.sleep(5000);
                }
                catch(Exception e){
                    e.printStackTrace();
                }
            }
//            for(int i=0;i<DefaultMessageStoreImpl.apcount.length;i++){
//                System.out.println("apcount "+i+":"+apcount[i]);
//            }
//            for(int i=0;i<MergeThread.acount.length;i++){
//                System.out.println("acount "+i+":"+MergeThread.acount[i]);
//            }
//            System.out.println(DefaultMessageStoreImpl.todump[0].size()+","+DefaultMessageStoreImpl.todump[1].size()+","+
//                    DefaultMessageStoreImpl.todump[2].size()+","+
//                    DefaultMessageStoreImpl.todump[3].size()+","+
//                    DefaultMessageStoreImpl.todump[4].size()+","+
//                    DefaultMessageStoreImpl.todump[5].size()+","+
//                    DefaultMessageStoreImpl.todump[6].size()+","+
//                    DefaultMessageStoreImpl.todump[7].size()+","+
//                    DefaultMessageStoreImpl.todump[8].size()+","+
//                    DefaultMessageStoreImpl.todump[9].size()+","+
//                    DefaultMessageStoreImpl.todump[10].size()+","+DefaultMessageStoreImpl.todump[11].size());
            //System.out.println(MergeThread.tcount[0]);
//            for(int i=0;i<1000;i++){
//                System.out.println("tmax "+i+":" + tmax[0][i]);
//            }

        }

        ByteBuffer fetchbufferta = fetchbuffertas[fetchtid.get()];
        ByteBuffer fetchbufferbody = fetchbufferbodys[fetchtid.get()];

        int indext1 = 0;
        for(;indext1<DefaultMessageStoreImpl.nparitions-1;indext1++){
            if(aMin<=MergeThread.singlestep*(indext1+1)){
                break;
            }
        }
        int indext2 = 0;
        for(;indext2<DefaultMessageStoreImpl.nparitions-1;indext2++){
            if(aMax<=MergeThread.singlestep*(indext2+1)){
                break;
            }
        }
        //System.out.println("idnext:"+indext1+","+indext2+MergeThread.acount[0]+","+MergeThread.acount[1]+","+aMin+","+aMax);
        for(int i=indext1;i<=indext2;i++) {
            int left = findLeftBound(tMin, tmax[i], MergeThread.tcount[i] - 1);//value of left-1  strictly smaller than amin; value of left >= amin;
            int right = findRightBound(tMax, tmax[i], MergeThread.tcount[i] - 1);//value of right strictly larger than amax; value of right-1 <=amx;
            //System.out.println("got:"+left+","+right+","+tmax[i][0]+","+tmax[i][MergeThread.tcount[i]-1]+","+aMin+","+aMax);
            if (left != -1 && right != -1) {
                //System.out.println("got1:"+left+","+right+","+tmax[i][left]+","+tmax[i][right]+","+aMin+","+aMax);
                int leftstart = left * nsmgsinglebatch;//第leftstart个数
                //int rightend = (right + 1) * nsmgsinglebatch;
                int rightend = Math.min((right + 1) * nsmgsinglebatch, MergeThread.total[i]);
                byte[] b = cache;
                long stagenum = 0;
                if (left == 0) {
                    stagenum = cacheparttval[i*MergeThread.cahcepartstepcnt];
                } else {
                    stagenum = tmax[i][left - 1];
                }
                long leftval = 0;
                while (leftstart <= rightend) {
                    long t = 0;
                    int kt = getkey3(cacheparttkey,i*MergeThread.cahcepartstepcnt, i*MergeThread.cahcepartstepcnt + cacheparttcount[i], leftstart);
                    if (kt != -1) {
                        t = cacheparttval[kt];
                    } else {
                        t = stagenum + b[i*MergeThread.stepcnt + leftstart] + 128;
                    }
                    stagenum = t;
                    if (t >= tMin) {
                        leftval = t;
                        break;
                    }
                    leftstart++;
                }

                if (tmax[i][MergeThread.tcount[i] - 1] <= tMax) {
                    rightend =  MergeThread.total[i];
                } else {
                    stagenum = 0;
                    if (right == 0) {
                        stagenum = cacheparttval[i*MergeThread.cahcepartstepcnt];
                    } else {
                        stagenum = tmax[i][right - 1];
                    }
                    int rightstart = right * nsmgsinglebatch;
                    while (rightstart <= rightend) {
                        long t = 0;
                        int kt = getkey3(cacheparttkey,i*MergeThread.cahcepartstepcnt, i*MergeThread.cahcepartstepcnt + cacheparttcount[i], rightstart);
                        if (kt != -1) {
                            t = cacheparttval[kt];
                        } else {
                            t = stagenum + b[i*MergeThread.stepcnt + rightstart] + 128;
                        }
                        stagenum = t;
                        if (t > tMax) {
                            break;
                        }
                        rightstart++;
                    }
                    rightend = rightstart;
                }
//                if(left!=0)
//                    System.out.println("start:"+leftstart+","+rightend+","+leftval+","+tmax[i][left-1]+","+tmax[i][right]+","+aMin+","+aMax);
//                //System.out.println();
                if (rightend > leftstart) {
                    //System.out.println("jump:"+leftstart+","+rightend);
                    try {
                        fetchbufferta.position(0);
                        fetchbufferbody.position(0);
                        fetchbufferta.limit((rightend - leftstart) * 6);
                        fetchbufferbody.limit((rightend - leftstart) * msglen2);
                        fcta2[i].read(fetchbufferta, (long) leftstart * 6);
                        fcbody2[i].read(fetchbufferbody, (long) leftstart * msglen2);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    demsg(res, aMin, aMax, fetchbufferta, fetchbufferbody, leftstart, leftval, b, i);
                }
                //xiang deng zuo pan duan
            }
        }
        Collections.sort(res, new Comparator<Message>() {
            @Override
            public int compare(Message o1, Message o2) {
                if(o1.getT()<o2.getT()){
                    return -1;
                }
                else if(o1.getT()>o2.getT()){
                    return 1;
                }
                return 0;
            }
        });
        return res;
    }


    @Override
    public long getAvgValue(long aMin, long aMax, long tMin, long tMax) {
        if(avgtid.get()==null){
            System.out.println(Runtime.getRuntime().freeMemory()/1000000+"MB");
            avgtid.set(avgtcounter.getAndIncrement());
        }
        ByteBuffer buffer2ta = avgbuffers[avgtid.get()];
        long sum = 0;
        long total = 0;

        int indext1 = 0;
        for(;indext1<DefaultMessageStoreImpl.nparitions-1;indext1++){
            if(aMin<=MergeThread.singlestep*(indext1+1)){
                break;
            }
        }
        int indext2 = 0;
        for(;indext2<DefaultMessageStoreImpl.nparitions-1;indext2++){
            if(aMax<=MergeThread.singlestep*(indext2+1)){
                break;
            }
        }
        //System.out.println("idnext:"+indext1+","+indext2);
        for(int i=indext1;i<=indext2;i++) {
            //merge index for part
            int left = findLeftBound(tMin, tmax[i], MergeThread.tcount[i] - 1);//value of left-1  strictly smaller than amin; value of left >= amin;
            int right = findRightBound(tMax, tmax[i], MergeThread.tcount[i] - 1);//value of right strictly larger than amax; value of right-1 <=amx;
            //System.out.println(Thread.currentThread().getName()+",got:"+i+","+left+","+right+","+count[i]+","+tmax[i][left]+","+tmax[i][0]+","+tmax[i][count[i]-1 - count[i]%nbatch]+","+aat[i]);
            //System.out.println("got:"+left+","+right);
            if (left != -1 && right != -1) {
                // System.out.println("got:"+left+","+right+","+tmax[0]+","+tmax[MergeThread.tcount-1 - MergeThread.tcount%nbatch]);
                int leftstart = left * nsmgsinglebatch;//第leftstart个数
                //int rightend = (right + 1) * nsmgsinglebatch;
                int rightend = Math.min((right + 1) * nsmgsinglebatch, MergeThread.total[i]);
                byte[] b = cache;
                long stagenum = 0;
                if (left == 0) {
                    stagenum = cacheparttval[i*MergeThread.cahcepartstepcnt];
                } else {
                    stagenum = tmax[i][left - 1];
                }
                long leftval = 0;
                while (leftstart <= rightend) {
                    long t = 0;
                    int kt = getkey3(cacheparttkey,i*MergeThread.cahcepartstepcnt, i*MergeThread.cahcepartstepcnt + cacheparttcount[i], leftstart);
                    if (kt != -1) {
                        t = cacheparttval[kt];
                    } else {
                        t = stagenum + b[i*MergeThread.stepcnt + leftstart] + 128;
                    }
                    stagenum = t;
                    if (t >= tMin) {
                        leftval = t;
                        break;
                    }
                    leftstart++;
                }

                if (tmax[i][MergeThread.tcount[i] - 1] <= tMax) {
                    rightend = MergeThread.total[i];
                } else {
                    stagenum = 0;
                    if (right == 0) {
                        stagenum = cacheparttval[i*MergeThread.cahcepartstepcnt];
                    } else {
                        stagenum = tmax[i][right - 1];
                    }
                    int rightstart = right * nsmgsinglebatch;
                    while (rightstart <= rightend) {
                        long t = 0;
                        int kt = getkey3(cacheparttkey,i*MergeThread.cahcepartstepcnt, i*MergeThread.cahcepartstepcnt + cacheparttcount[i], rightstart);
                        if (kt != -1) {
                            t = cacheparttval[kt];
                        } else {
                            t = stagenum + b[i*MergeThread.stepcnt + rightstart] + 128;
                        }
                        stagenum = t;
                        if (t > tMax) {
                            break;
                        }
                        rightstart++;
                    }
                    rightend = rightstart;
                }
                // System.out.println("start:"+leftstart+","+rightend+","+leftval+","+tmax[left-1]+","+tmax[right]);

                if (rightend > leftstart) {
                    buffer2ta.position(0);
                    buffer2ta.limit((rightend - leftstart) * 6);
                    try {
                        fcta2[i].read(buffer2ta, (long) leftstart * 6);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                    int avgpos = leftstart;
                    buffer2ta.position(0);
                    if(i!=indext1&&i!=indext2){
                        while(avgpos<rightend){
                            if(avgpos%nsmgsinglebatch==0&&avgpos+nsmgsinglebatch<=rightend&&asum[i][avgpos/nsmgsinglebatch]>0){
                                sum+=asum[i][avgpos/nsmgsinglebatch];
                                total+=nsmgsinglebatch;
                                avgpos+=nsmgsinglebatch;
                                buffer2ta.position(buffer2ta.position()+nsmgsinglebatch*6);
                            }
                            else {
                                long a = getL(buffer2ta);
                                if (i == nparitions - 1) {
                                    int kt = DefaultMessageStoreImpl.getkey3(DefaultMessageStoreImpl.cachekey3, 0, cachecount3, avgpos);
                                    if (kt != -1) {
                                        a = DefaultMessageStoreImpl.cacheval3[kt];
                                    }
                                }
                                avgpos++;
                                total++;
                                sum += a;
                            }
                        }
                    }
                    else{
                        while(avgpos<rightend){
                            long a = getL(buffer2ta);
                            if(i==nparitions-1) {
                                int kt = DefaultMessageStoreImpl.getkey3(DefaultMessageStoreImpl.cachekey3, 0, cachecount3, avgpos);
                                if (kt != -1) {
                                    a = DefaultMessageStoreImpl.cacheval3[kt];
                                }
                            }
                            avgpos++;
                            if (a >= aMin && a <= aMax) {
                                total++;
                                sum += a;
                            }
                        }
                    }
                }
            }
        }
        //System.out.println(new Date()+":"+Thread.currentThread().getName()+","+aMin+","+aMax+","+tMin+","+tMax+","+sum+","+total);
        return total==0?0:sum/total;
    }

    public void enmsg(Message message, ByteBuffer b, int pindex){
        //b.putLong(message.getA());
        if(message.getA()>MergeThread.stepnum){
            cachekey2[pindex*cachepsize2 + cachecount2[pindex]] = total[pindex];
            cacheval2[pindex*cachepsize2 + cachecount2[pindex]++] = message.getA();
            b.putShort((short)0);
            b.putInt(0);
            if(pindex*cachepsize2 + cachecount2[pindex]==DefaultMessageStoreImpl.cachekey2.length){
                System.out.println(" rehashing a");
                int[] cacheparttkey2 = new int[DefaultMessageStoreImpl.cachekey2.length*2];
                long[] cacheparttval2 = new long[DefaultMessageStoreImpl.cachekey2.length*2];
                System.arraycopy(cachekey2, 0, cacheparttkey2, 0, DefaultMessageStoreImpl.cachekey2.length);
                System.arraycopy(cacheval2, 0, cacheparttval2, 0, DefaultMessageStoreImpl.cachekey2.length);
                DefaultMessageStoreImpl.cachepsize2*=2;
                DefaultMessageStoreImpl.cachekey2 = cacheparttkey2;
                DefaultMessageStoreImpl.cacheval2 = cacheparttval2;
            }
        }
        else {
            long a = message.getA();
            b.putShort((short)(a>>>32));
            b.putInt((int)a);
            //System.out.println("put:"+a+","+f);
        }
        b.put(message.getBody());
    }
    public int findLeftBound(long aMin, long[] tmax, int upper){
        //value of left-1  strictly smaller than amin; value of left >= amin;
        //System.out.println("start find left:"+upper+","+aMin+","+tmax[0]+","+tmax[upper]);
        if(aMin>tmax[upper]){
            return -1;
        }
        if(aMin<tmax[0]){
            return 0;
        }
        int left = 0;
        int right = upper;
        int mid = (left + right)/2;
        while(left<right){
           // System.out.println(left+","+right+","+mid);
            if(mid==0){
                if(tmax[0]<aMin){
                    return 1;
                }
                else{
                    return 0;
                }
            }
            if(mid==upper){
                if(tmax[upper]<aMin){
                    return -1;
                }
                else{
                    return upper;
                }
            }

            if(tmax[mid-1]<aMin && tmax[mid]>=aMin){
                return mid;
            }
            else if(tmax[mid-1]>=aMin){
                right = mid-1;
            }
            else if(tmax[mid]<aMin){
                left = mid+1;
            }
            mid = (left + right)/2;
        }
        return (left + right)/2;
    }
    public int findRightBound(long aMax,long[] tmax, int upper){
        //value of right strictly larger than amax; value of right-1 <=amx;
        if(aMax<0) return -1;
        if(aMax>=tmax[upper]){
            return upper;
        }
        if(aMax<tmax[0]){
            return 0;
        }
        int left = 0;
        int right = upper;
        int mid = (left + right)/2;
        while(left<right){
            if(mid==0){
                if(tmax[0]>aMax){
                    return 0;
                }
                else{
                    return 1;
                }
            }
            if(mid==upper){
                    return upper;
            }
            if(tmax[mid-1]<=aMax && tmax[mid] > aMax){
                return mid;
            }
            else if(tmax[mid-1] > aMax){
                right = mid-1;
            }
            else if(tmax[mid] <= aMax){
                left = mid+1;
            }
            mid = (left + right)/2;
        }
        return (left + right)/2;
    }
    public void demsg(ArrayList<Message> list, long aMin, long aMax, ByteBuffer buffer2ta, ByteBuffer buffer2body, int leftstart, long leftval, byte[] b,int i){
        buffer2ta.position(0);
        buffer2body.position(0);
        while(buffer2ta.remaining()>0){
            long t = leftval;
            //long a = ((long) buffer2ta.get() + 128) << 40 | ((long) buffer2ta.get() + 128) << 32 | ((long) buffer2ta.get() + 128) << 24 | ((long) buffer2ta.get() + 128) << 16 | ((long) buffer2ta.get() + 128) << 8 | ((long) buffer2ta.get() + 128);
            //long a = ((long)buffer2ta.getShort()<<31)|(long)buffer2ta.getInt() + stepfetch[i];
            long a = getL(buffer2ta);
            if(i==nparitions-1) {
                int kta = DefaultMessageStoreImpl.getkey3(DefaultMessageStoreImpl.cachekey3, 0, cachecount3, leftstart);
                if (kta != -1) {
                    a = DefaultMessageStoreImpl.cacheval3[kta];
                }
            }
            leftstart++;
            int kt = getkey3(cacheparttkey,i*MergeThread.cahcepartstepcnt, i*MergeThread.cahcepartstepcnt + cacheparttcount[i], leftstart);
            if (kt != -1) {
                t = cacheparttval[kt];
            } else {
                leftval = leftval + b[i*MergeThread.stepcnt + leftstart] + 128;
            }


            //long a = buffer2ta.getLong();
            //System.out.println("de:"+t+","+a+","+i);
            if(a >= aMin && a <= aMax){
                byte[] body = new byte[34];
                buffer2body.get(body);
                Message msg = new Message(a, t, body);
                //System.out.println("de:"+msg.getT()+","+msg.getA());
                list.add(msg);
            }
            else{
                buffer2body.position(buffer2body.position()+34);
            }
        }
    }
    public static void cachemsg(byte tstep, byte[] b, int total) {
        b[total] = tstep;
    }
    public static int getkey(int[] num,int upper, int target){
        int left = 0;
        int right = upper-1;
        int mid = (left+right)/2;
        while(left<=right){
            if(num[mid] == target){
                return mid;
            }
            else if(num[mid] < target){
                left = mid+1;
            }
            else{
                right = mid-1;
            }
            mid = (left + right)/2;
        }
        return -1;
    }
    public static int getkey3(int[] num,int lower, int upper, int target){
        int left = lower;
        int right = upper-1;
        int mid = (int)(((long)left+(long)right)/2L);
        while(left<=right){
            if(num[mid] == target){
                return mid;
            }
            else if(num[mid] < target){
                left = mid+1;
            }
            else{
                right = mid-1;
            }
            mid = (int)(((long)left+(long)right)/2L);
        }
        return -1;
    }

    public static long getL(ByteBuffer b) {
        short s = b.getShort();
        int s2 = b.getInt();
        return (((long)(s&0x8000)>>>15))<<47|((long)((s2&0x80000000)>>>31))<<31|((long)(s&0x7fff))<<32|(long)(s2&0x7fffffff);
    }

}
class WTHread extends Thread{
    private static volatile int nstep;
    private int wid;
    public static volatile ConcurrentLinkedQueue<wByteBuffer2>[] toput;
    public static volatile ConcurrentLinkedQueue<wByteBuffer2>[] todump;
    public static volatile FileChannel[] fc;
    public static volatile boolean putdone = false;
    public volatile boolean done = false;
    private int wcounter = 0;
    public WTHread(int wid, int nstep){
        this.wid = wid;
        this.nstep = nstep;
    }

    @Override
    public void run() {
        while(true){
            //System.out.println(Thread.currentThread().getName()+":"+todump[wid * nstep].size()+","+todump[wid * nstep + 1].size());
            int index = wid * nstep + wcounter;
            wcounter = (wcounter+1)%nstep;
            if(!todump[index].isEmpty()){
                //System.out.println(Thread.currentThread().getName()+":"+todump[wid * nstep].size()+","+todump[wid * nstep + 1].size());
                wByteBuffer2 buffers = todump[index].poll();
                ByteBuffer buffer = buffers.buffer;
                buffer.position(0);
                try {
                    fc[index].write(buffer);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                buffer.position(0);
                toput[index].offer(buffers);
                try {
                    Thread.sleep(0, 1);
                }
                catch(Exception e){
                    e.printStackTrace();
                }
            }
            else{
                if(putdone) {
                    int i=0;
                    for(;i<nstep;i++){
                        if(!todump[wid * nstep + i].isEmpty()){
                            break;
                        }
                    }
                    if (i==nstep) {
                        break;
                    }
                }
                else{
                    try {
                        Thread.sleep(500);
                    }
                    catch(Exception e){
                        e.printStackTrace();
                    }
                }
            }
        }
        done = true;
    }
}
class MergeThread extends Thread{
    //for read
    public static final int nthreads = 12;
    public static volatile boolean dumpdone = false;
    public int[] pos = new int[nthreads];
    public static int[] posa = new int[nthreads];
    public int[] count = new int[nthreads];
    public ByteBuffer[] databody = new ByteBuffer[nthreads];
    public ByteBuffer[] datata = new ByteBuffer[nthreads];
    public static volatile ConcurrentLinkedQueue<wByteBuffer>[] toput2;
    public static volatile ConcurrentLinkedQueue<wByteBuffer>[] todump2;
    public static volatile boolean putdone[] = new boolean[nthreads];
    public static volatile boolean mdone[] = new boolean[nthreads];
    public static volatile int nmsg;
    public volatile long[] ttmp = new long[nthreads];

    //for write and merge
    public static volatile ByteBuffer[] writeBufferBody = new ByteBuffer[DefaultMessageStoreImpl.nparitions];
    public static volatile ByteBuffer[] writeBufferTa = new ByteBuffer[DefaultMessageStoreImpl.nparitions];
    private static final int nsmgsinglebatch = DefaultMessageStoreImpl.nsmgsinglebatch;
    private static byte[] body = new byte[34];
    private long[] pre = new long[DefaultMessageStoreImpl.nparitions];//-1;
    public static volatile int[] mcount = new int[DefaultMessageStoreImpl.nparitions]; //tmp count
    public static volatile int[] tcount = new int[DefaultMessageStoreImpl.nparitions]; //index count
    public static volatile int[] total = new int[DefaultMessageStoreImpl.nparitions];  //msg count
    public static volatile FileChannel[] fcta2;
    public static volatile FileChannel[] fcbody2;
    int ccount = 0;
    public static long stepnum = Long.MAX_VALUE>>DefaultMessageStoreImpl.msglen1>>(DefaultMessageStoreImpl.msglen1-1);
    public static long singlestep = stepnum/DefaultMessageStoreImpl.nparitions;
    //public static int stepcnt = 1050000000;
    //public static int stepcnt = 525000000;
    public static int stepcnt = 2120000000/DefaultMessageStoreImpl.nparitions;
    public static int cahcepartstepcnt = nthreads*DefaultMessageStoreImpl.cacheparttsize/DefaultMessageStoreImpl.nparitions;


    //cache相等

    public MergeThread(int nmsg, ConcurrentLinkedQueue<wByteBuffer>[] toput, ConcurrentLinkedQueue<wByteBuffer>[] todump, FileChannel[] fcta2, FileChannel[] fcbody2){
        this.nmsg = nmsg;
        this.toput2 = toput;
        this.todump2 = todump;
        this.fcta2 = fcta2;
        this.fcbody2 = fcbody2;
        for(int i=0;i<DefaultMessageStoreImpl.nparitions;i++) {
            writeBufferBody[i] = ByteBuffer.allocateDirect(nmsg * 34);
            writeBufferTa[i] = ByteBuffer.allocateDirect(nmsg * 6);
            pre[i] = -1;
        }
    }


    @Override
    public void run(){
        while(true){
            int nqueue = 0;
            int minindex = -1;
            long mint = Long.MAX_VALUE;
            for(int i=0;i<nthreads;i++){
                //System.out.println(i+":blocking");
                if(mdone[i]){
                    nqueue++;
                }
                else {
                    if(count[i]==0){
                        while (todump2[i].isEmpty()) {
                            try {
                                Thread.sleep(0, 1);
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                        wByteBuffer wbuffer = todump2[i].poll();
                        datata[i] = wbuffer.bufferta;
                        databody[i] = wbuffer.bufferbody;
                        count[i] = wbuffer.count;
                        pos[i] = 0;
                        datata[i].position(0);
                        databody[i].position(0);
                        long t = datata[i].getLong();
                        if (minindex == -1 || t < mint) {
                            minindex = i;
                            mint = t;
                        }
                        ttmp[i] = t;
                        ccount++;
                    }
                    else{
                        long t = ttmp[i];
                        if (minindex == -1 || t < mint) {
                            minindex = i;
                            mint = t;
                        }
                    }
                }
            }
            if(nqueue==nthreads){
                break;
            }
            else{
                long t = ttmp[minindex];
//                for(int i=0;i<12;i++){
//                    System.out.println("remaining i:"+datata[i].remaining()+","+pos[i]+","+count[i]+","+ccount);
//                }
//                if(t<10000)
//                    System.out.println("merge t:" + t+","+datata[minindex].remaining()+","+minindex+","+total[0]+","+ccount);
                //long a = ((long) databody[minindex].get() + 128) << 40 | ((long) databody[minindex].get() + 128) << 32 | ((long) databody[minindex].get() + 128) << 24 | ((long) databody[minindex].get() + 128) << 16 | ((long) databody[minindex].get() + 128) << 8 | ((long) databody[minindex].get() + 128);
                //long a = ((long)databody[minindex].getShort()<<31)|(long)databody[minindex].getInt() + DefaultMessageStoreImpl.stepput[minindex];
                long a = DefaultMessageStoreImpl.getL(databody[minindex]);
                int kt = DefaultMessageStoreImpl.getkey3(DefaultMessageStoreImpl.cachekey2, minindex*DefaultMessageStoreImpl.cachepsize2, minindex*DefaultMessageStoreImpl.cachepsize2+DefaultMessageStoreImpl.cachecount2[minindex], posa[minindex]);
                if(kt!=-1){
                    a = DefaultMessageStoreImpl.cacheval2[kt];
                }

                databody[minindex].get(body);
                pos[minindex]++;
                posa[minindex]++;
                if (pos[minindex] >= count[minindex]) {
                    datata[minindex].position(0);
                    databody[minindex].position(0);
                    toput2[minindex].offer(new wByteBuffer(datata[minindex], databody[minindex], 0));

                    if (todump2[minindex].isEmpty()) {
                        if (putdone[minindex]) {
                            mdone[minindex] = true;
                        } else {
                            //not put done, waiting for buffer
                            while (!putdone[minindex] && todump2[minindex].isEmpty()) {
                                try {
                                    Thread.sleep(0, 1);
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            }
                            //hou mian gan sha
                            if (putdone[minindex]) {
                                mdone[minindex] = true;
                            } else {
                                wByteBuffer wbuffer = todump2[minindex].poll();
                                datata[minindex] = wbuffer.bufferta;
                                databody[minindex] = wbuffer.bufferbody;
                                count[minindex] = wbuffer.count;
                                pos[minindex] = 0;
                                datata[minindex].position(0);
                                databody[minindex].position(0);
                                ttmp[minindex] = datata[minindex].getLong();
                                ccount++;
                            }
                        }
                    } else {
                        //fetch buffer from queue
                        wByteBuffer wbuffer = todump2[minindex].poll();
                        datata[minindex] = wbuffer.bufferta;
                        databody[minindex] = wbuffer.bufferbody;
                        count[minindex] = wbuffer.count;
                        pos[minindex] = 0;
                        datata[minindex].position(0);
                        databody[minindex].position(0);
                        ttmp[minindex] = datata[minindex].getLong();
                        ccount++;
                    }
                } else {
                    //fetch next msg
                    ttmp[minindex] = datata[minindex].getLong();
                    ccount++;
                }

                int indext = 0;
                for(;indext<DefaultMessageStoreImpl.nparitions-1;indext++){
                    if(a<=singlestep*(indext+1)){
                        break;
                    }
                }
                //acount[indext]++;
//                if(acount[indext]==DefaultMessageStoreImpl.cachesize/DefaultMessageStoreImpl.nparitions*nthreads){
//                    for(int y=0;y<DefaultMessageStoreImpl.nparitions;y++)
//                        System.out.println("acount:"+acount[y]);
//                }
                if (pre[indext] != -1) {
                    int tstep = (int)(t - pre[indext]);
                    if(tstep<=DefaultMessageStoreImpl.cachetend){
                        cachemsg((byte)(tstep-128), DefaultMessageStoreImpl.cache, (int)(indext*stepcnt) + total[indext]);
                    }
                    else{
                        DefaultMessageStoreImpl.cacheparttkey[indext*cahcepartstepcnt+DefaultMessageStoreImpl.cacheparttcount[indext]] = total[indext];
                        DefaultMessageStoreImpl.cacheparttval[indext*cahcepartstepcnt+DefaultMessageStoreImpl.cacheparttcount[indext]++] = t;
                        if(indext*cahcepartstepcnt+DefaultMessageStoreImpl.cacheparttcount[indext]==DefaultMessageStoreImpl.cacheparttval.length){
                            System.out.println(" merge rehashing");
                            int[] cacheparttkey2 = new int[nthreads*DefaultMessageStoreImpl.cacheparttsize*2];
                            long[] cacheparttval2 = new long[nthreads*DefaultMessageStoreImpl.cacheparttsize*2];
                            System.arraycopy(DefaultMessageStoreImpl.cacheparttkey, 0, cacheparttkey2, 0, DefaultMessageStoreImpl.cacheparttsize*nthreads);
                            System.arraycopy(DefaultMessageStoreImpl.cacheparttval, 0, cacheparttval2, 0, DefaultMessageStoreImpl.cacheparttsize*nthreads);
                            DefaultMessageStoreImpl.cacheparttsize*=2;
                            DefaultMessageStoreImpl.cacheparttkey = cacheparttkey2;
                            DefaultMessageStoreImpl.cacheparttval = cacheparttval2;
                        }
                    }
                }
                else{
                    DefaultMessageStoreImpl.cacheparttkey[indext*cahcepartstepcnt+DefaultMessageStoreImpl.cacheparttcount[indext]] = total[indext];
                    DefaultMessageStoreImpl.cacheparttval[indext*cahcepartstepcnt+DefaultMessageStoreImpl.cacheparttcount[indext]++] = t;
                    if(indext*cahcepartstepcnt+DefaultMessageStoreImpl.cacheparttcount[indext]==DefaultMessageStoreImpl.cacheparttval.length){
                        System.out.println(" merge rehashing2");
                        int[] cacheparttkey2 = new int[nthreads*DefaultMessageStoreImpl.cacheparttsize*2];
                        long[] cacheparttval2 = new long[nthreads*DefaultMessageStoreImpl.cacheparttsize*2];
                        System.arraycopy(DefaultMessageStoreImpl.cacheparttkey, 0, cacheparttkey2, 0, DefaultMessageStoreImpl.cacheparttsize*nthreads);
                        System.arraycopy(DefaultMessageStoreImpl.cacheparttval, 0, cacheparttval2, 0, DefaultMessageStoreImpl.cacheparttsize*nthreads);
                        DefaultMessageStoreImpl.cacheparttsize*=2;
                        DefaultMessageStoreImpl.cacheparttkey = cacheparttkey2;
                        DefaultMessageStoreImpl.cacheparttval = cacheparttval2;
                    }
                }

                pre[indext] = t;
                //writeBufferTa[indext].putLong(a);
                if(a<=stepnum){
//                    System.out.println("merge a:"+a+","+(a-DefaultMessageStoreImpl.stepfetch[indext]));
//                    a -= DefaultMessageStoreImpl.stepfetch[indext];
                    writeBufferTa[indext].putShort((short)(a>>>32));
                    writeBufferTa[indext].putInt((int)a);

                }
                else{
                    DefaultMessageStoreImpl.cachekey3[DefaultMessageStoreImpl.cachecount3] = total[indext];
                    DefaultMessageStoreImpl.cacheval3[DefaultMessageStoreImpl.cachecount3++] = a;
                    writeBufferTa[indext].putShort((short)0);
                    writeBufferTa[indext].putInt(0);
                    if(DefaultMessageStoreImpl.cachecount3==DefaultMessageStoreImpl.cachekey3.length){
                        System.out.println(" rehashing2 a");
                        int[] cacheparttkey2 = new int[DefaultMessageStoreImpl.cachekey3.length*2];
                        long[] cacheparttval2 = new long[DefaultMessageStoreImpl.cachekey3.length*2];
                        System.arraycopy(DefaultMessageStoreImpl.cachekey3, 0, cacheparttkey2, 0, DefaultMessageStoreImpl.cachekey3.length);
                        System.arraycopy(DefaultMessageStoreImpl.cacheval3, 0, cacheparttval2, 0, DefaultMessageStoreImpl.cachekey3.length);
                        DefaultMessageStoreImpl.cachepsize2*=2;
                        DefaultMessageStoreImpl.cachekey3 = cacheparttkey2;
                        DefaultMessageStoreImpl.cacheval3 = cacheparttval2;
                    }
                }
                total[indext]++;
                writeBufferBody[indext].put(body);
                mcount[indext]++;
                DefaultMessageStoreImpl.asum[indext][tcount[indext]] +=a;
                if(mcount[indext]%nsmgsinglebatch==0){
                   // System.out.println("merge t:"+t+","+tcount[indext]);
                    DefaultMessageStoreImpl.tmax[indext][tcount[indext]++] = t;
                }
                if(mcount[indext]==nmsg){
                    writeBufferTa[indext].position(0);
                    writeBufferBody[indext].position(0);
                    try {
                        fcta2[indext].write(writeBufferTa[indext]);
                        fcbody2[indext].write(writeBufferBody[indext]);
                    }
                    catch(Exception e){
                        e.printStackTrace();
                    }
                    writeBufferTa[indext].position(0);
                    writeBufferBody[indext].position(0);
                    mcount[indext] = 0;
                }
            }
        }
        //merge dump
        for(int m=0;m<DefaultMessageStoreImpl.nparitions;m++){
            if(mcount[m]%nsmgsinglebatch!=0){
                DefaultMessageStoreImpl.tmax[m][tcount[m]++] = pre[m];
            }
            if(MergeThread.mcount[m]>0){
                MergeThread.writeBufferTa[m].position(0);
                MergeThread.writeBufferTa[m].limit(MergeThread.mcount[m]*6);
                MergeThread.writeBufferBody[m].position(0);
                MergeThread.writeBufferBody[m].limit(MergeThread.mcount[m]*DefaultMessageStoreImpl.msglen2);
                try {
                    fcta2[m].write(MergeThread.writeBufferTa[m]);
                    fcbody2[m].write(MergeThread.writeBufferBody[m]);
                }
                catch(Exception e){
                    e.printStackTrace();
                }
            }
        }
        dumpdone = true;
        System.out.println("cccount:"+ccount);
    }
    public static void cachemsg(byte tstep, byte[] b, int total) {
        b[total] = tstep;
    }
}
class wByteBuffer{
    ByteBuffer bufferta;
    ByteBuffer bufferbody;
    int count;
    public wByteBuffer(ByteBuffer bufferta, ByteBuffer bufferbody, int count){
        this.bufferta = bufferta;
        this.bufferbody = bufferbody;
        this.count = count;
    }
}
class PutThread extends Thread{
    private static final int nthreads = 12;
    public static volatile ConcurrentLinkedQueue<wByteBuffer>[] toput2;
    public static volatile ConcurrentLinkedQueue<wByteBuffer>[] todump2;
    public static volatile int[] putcount;
    private static volatile ByteBuffer[] datata2 = new ByteBuffer[nthreads];
    private static volatile ByteBuffer[] databody2 = new ByteBuffer[nthreads];
    private int pid;
    private static int nmsg;
    public static volatile FileChannel[] fcm;
    public static volatile FileChannel[] fc;

    public PutThread(int pid){
        this.pid = pid;
        this.toput2 = DefaultMessageStoreImpl.toput2;
        this.todump2 = DefaultMessageStoreImpl.todump2;
        this.putcount = DefaultMessageStoreImpl.putcount;
        this.nmsg = DefaultMessageStoreImpl.nmsg;
        this.fcm = DefaultMessageStoreImpl.fcm;
        this.fc = DefaultMessageStoreImpl.fc;
        //System.out.println("putcount:"+putcount[pid]);
    }

    @Override
    public void run(){

        wByteBuffer bbs = toput2[pid].poll();
        datata2[pid] = bbs.bufferta;
        databody2[pid] = bbs.bufferbody;
        try {
            fcm[pid].position(0);
            fc[pid].position(0);
        }
        catch(Exception e){
            e.printStackTrace();
        }
        int i = 0;
        while(true){
            try {
                fcm[pid].read(datata2[pid]);
                fc[pid].read(databody2[pid]);
            }
            catch(Exception e){
                e.printStackTrace();
            }
            datata2[pid].position(0);
            databody2[pid].position(0);
            bbs.count = nmsg;
            todump2[pid].offer(bbs);

            i++;
            //System.out.println(new Date()+","+Thread.currentThread().getName()+":"+i);
            if(i==putcount[pid]){
                if(DefaultMessageStoreImpl.tmpcount[pid]>0){
                    todump2[pid].offer(new wByteBuffer(DefaultMessageStoreImpl.buffermerges[pid], DefaultMessageStoreImpl.data[pid], DefaultMessageStoreImpl.tmpcount[pid]));
                }
                MergeThread.putdone[pid] = true;
                System.out.println(new Date()+","+Thread.currentThread().getName()+": put done."+i);
                break;
            }
            else{
                while(toput2[pid].isEmpty()){
                    // System.out.println(new Date()+","+Thread.currentThread().getName()+"waiting for dump."+toput[fetchtid.get()].size()+","+todump[fetchtid.get()].size());
                    try {
                        Thread.sleep(0, 1);
                    }
                    catch(Exception e){
                        e.printStackTrace();
                    }
                }
                bbs = toput2[pid].poll();
                datata2[pid] = bbs.bufferta;
                databody2[pid] = bbs.bufferbody;
            }
        }
    }
}
class wByteBuffer2{
    ByteBuffer buffer;
    int count;
    public wByteBuffer2(ByteBuffer buffer, int count){
        this.buffer = buffer;
        this.count = count;
    }
}