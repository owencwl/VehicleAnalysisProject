package com.umxwe.genetedata.utils;


import com.umxwe.genetedata.threadpool.ThreadPoolManager;
import com.umxwe.common.hbase.exception.HBaseInsertException;
import org.apache.hadoop.hbase.client.Put;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.umxwe.common.utils.TimeUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * @ClassName DataProducer
 * @Description Todo
 * @Author owen(umxwe))
 * @Data 2020/9/18
 */
public class ConcurrentDataProducerutil {
    private static final Logger logger = LoggerFactory.getLogger(ConcurrentDataProducerutil.class);
    private static List<Exception> exceptions = Collections.synchronizedList(new ArrayList<Exception>());

    /**
     * 批量生成原始数据
     *
     * @param listNum
     * @param t
     * @param <T>
     * @return
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    public static <T> List<T> producerListData(int listNum, Class<T> t) throws Exception {
//        logger.info("##########################开始线程生成数据##########################");
        TimeUtils.now();
        List<T> list = new ArrayList<T>();
        for (int i = 0; i < listNum; i++) {
            T o = t.newInstance();
            list.add(o);
        }
//        logger.info("##########################线程生成数据耗时：" + (TimeUtils.timeInterval()) + "ms ##########################");
        return list;
    }

    /**
     * 多线程生成原始数据
     *
     * @param listNum
     * @param t
     * @param <T>
     * @return
     */
    public static <T> List<T> producerListDataByMutlThread(int listNum, Class<T> t) throws Exception {
        TimeUtils.now();
        int preThreadNum = 10;
        ExecutorService executorService = ThreadPoolManager.getInstance().getExecutorServer(preThreadNum);

        //结果集
        List<T> list = new ArrayList<T>(listNum);
        List<Future<List<T>>> futureList = new ArrayList<Future<List<T>>>(preThreadNum);

        int index = 0;
        int dealSize = 2000;

        //分配
        for(int i=0;i<= preThreadNum;i++,index+=dealSize){
            int start = index;
            if(start>=listNum) break;
            int end = start + dealSize;
            end = end>listNum ? listNum : end;
            futureList.add(executorService.submit(new CallableTask(start,end,t)));

//            FutureTask<List<T>> futureTask = new FutureTask<List<T>>(new CallableTask(list,start,end));
        }



        for (Future<List<T>> future : futureList) {
            while (true) {
                if (future.isDone() && !future.isCancelled()) {
                    List<T> futureResultList =  future.get();//获取结果
                    list.addAll(futureResultList);
                    break;
                } else {
                    //每次轮询休息1毫秒（CPU纳秒级），避免CPU高速轮循耗空CPU
                    Thread.sleep(1);
                }
            }
        }
        logger.info("多线程生成原始数据总耗时： "+TimeUtils.timeInterval()+" ms");

        return list;
    }


    /**
     * 多线程写入hbase数据
     *
     * @param hbaseTable
     * @param puts
     * @return
     */
    public static void insertListDataByMutlThread(String hbaseTable, List<Put> puts) throws Exception {
        ConcurrentDataProducerutil.insertHbase(hbaseTable, puts);
    }


    public static void insertHbase(String hbaseTable, List<Put> listData) throws Exception {
        //-Dlog4j.configuration=log4j/log4j.properties
//        PropertyConfigurator.configure("src/main/resources/log4j/log4j.properties");

        int preThreadNum = 20;
        int size = listData.size();

        TimeUtils.now();

        int threadNum = (int) Math.ceil(size / preThreadNum);

        CountDownLatch countDownLatch = new CountDownLatch(threadNum);
        ExecutorService executorService = ThreadPoolManager.getInstance().getExecutorServer(threadNum);

        logger.info("开启 " + threadNum + " 个线程 " + "生产数据 . ");


        try {
            for (int i = 0; i < threadNum; i++) {

                final List<Put> tempList;
                // 数据切分
                if (i == (threadNum - 1)) {
                    tempList = listData.subList(preThreadNum * i, size);
                } else {
                    tempList = listData.subList(preThreadNum * i, preThreadNum * (i + 1));
                }
                executorService.execute(new TaskProducer(countDownLatch, hbaseTable, tempList));
            }
            countDownLatch.await();
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            executorService.shutdown();
        }

        if(exceptions.size()>0){
            HBaseInsertException insertExceptor = new HBaseInsertException(String.format("put 数据到表%s失败", hbaseTable));
            insertExceptor.addSuppresseds(exceptions);
            throw insertExceptor;
        }

        long timeInterval = TimeUtils.timeInterval();
        logger.info(" 多线程写入Hbase，一共耗时  ：" + timeInterval + "  毫秒 ... ");


    }


    private static class TaskProducer implements Runnable {
        private CountDownLatch countDownLatch;
        private List<Put> listData;
        private String hbaseTable;

        public TaskProducer(CountDownLatch countDownLatch, String hbaseTable, List<Put> listData) {
            this.countDownLatch = countDownLatch;
            this.listData = listData;
            this.hbaseTable = hbaseTable;
        }

        @Override
        public void run() {
            try {
                /**
                 * 这里主要为业务逻辑
                 */
//                HBaseTableUtil.insertManyData(hbaseTable, listData);
            } catch (Exception e) {
                e.printStackTrace();
                exceptions.add(e);
            } finally {
                countDownLatch.countDown();
            }
        }
    }

    private static class CallableTask<T> implements Callable<List<T>> {
        private int num;
        private Class<T> bean;

        public CallableTask(int start,int end,Class<T> bean) {
            this.num=end-start;
            this.bean=bean;
        }


        @Override
        public List<T> call() throws Exception {
            return ConcurrentDataProducerutil.producerListData(num,bean);
        }
    }
}
