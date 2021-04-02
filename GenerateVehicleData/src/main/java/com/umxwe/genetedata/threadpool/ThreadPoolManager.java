package com.umxwe.genetedata.threadpool;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @ClassName ThreadPoolManager
 * @Description Todo
 * @Author owen(umx)
 * @Data 2020/9/18
 */
public class ThreadPoolManager {
    private static ThreadPoolManager tpm;
    private transient ExecutorService newCacheThreadPool;
    private int poolcapicity;
    private transient ExecutorService newFixThreadPool;


    public ThreadPoolManager() {
    }

    public static ThreadPoolManager getInstance() {

        if (tpm == null) {
            synchronized (ThreadPoolManager.class) {
                if (tpm == null) {
                    tpm = new ThreadPoolManager();
                }
            }
        }
        return tpm;
    }

    public ExecutorService getExecutorServer() {
        if (newCacheThreadPool == null) {
            synchronized (ThreadPoolManager.class) {
                if (newCacheThreadPool == null) {

                    //newFixedThreadPool newCachedThreadPool
                    newCacheThreadPool = Executors.newCachedThreadPool();
                }
            }
        }
        return newCacheThreadPool;
    }

    public ExecutorService getExecutorServer(int poolcapcity) {
        return getExecutorService(poolcapcity, false);
    }

    private synchronized ExecutorService getExecutorService(int poolcapcity, boolean clodeOld) {
        if (newFixThreadPool == null || this.poolcapicity != poolcapcity) {
            if (newFixThreadPool != null && clodeOld) {
                newFixThreadPool.shutdown();
            }

            newFixThreadPool = Executors.newFixedThreadPool(poolcapcity);
            this.poolcapicity = poolcapcity;
        }
        return newFixThreadPool;
    }
}
