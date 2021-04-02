package com.umxwe.common.hbase.utils;

/**
 * @ClassName SimplePool
 * @Description Todo
 * @Author owen(umxwe)
 * @Date 2021/2/5
 */

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * SimplePool是简单的资源对象管理池， 可以将Closable的资源类实例放在此池中进行管理
 *
 * @param <T>
 */
public class SimplePool<T> {
    // 回收资源的方法名
    private String closemethod = "close";
    boolean cglib = false;
    private LinkedBlockingQueue<T> sources = new LinkedBlockingQueue<T>();
    private List<T> origins = new ArrayList<T>();

    public SimplePool() {
    }

    /**
     * SimplePool 支持用cglib和JDK动态代理两种方式， closemethodname用户指定回收资源的方法名
     *
     * @param cglib
     * @param closemethodname
     */
    public SimplePool(boolean cglib, String closemethodname) {
        this.cglib = cglib;
        this.closemethod = closemethodname;
    }

    // cglib intereeptor
//    class Interceptor implements MethodInterceptor {
//        T target;
//        Interceptor(T t) {
//            target = t;
//        }
//        @Override
//        public Object intercept(Object o, Method method, Object[] objects, MethodProxy methodProxy) throws Throwable {
//            if (method.getName().equalsIgnoreCase(SimplePool.this.closemethod)) {
//                sources.put((T) o);
//                return null;
//            }
//            return method.invoke(target, objects);
//        }
//    }

    public T getSource() throws InterruptedException {
        return this.sources.take();
    }

    /**
     * 将资源加入到池中
     *
     * @param t
     */
    public synchronized void addSource(T t) {
        origins.add(t);
        if (cglib) {
//            Interceptor interceptor = new Interceptor(t);
//            Enhancer enhancer = new Enhancer();
//            enhancer.setSuperclass(t.getClass());
//            enhancer.setCallback(new Interceptor(t));
//            T proxy = (T) enhancer.create();
//            sources.offer(proxy);
        } else {
            Handler handler = new Handler(t);
            T pt = (T) Proxy.newProxyInstance(t.getClass().getClassLoader(), t.getClass().getInterfaces(), handler);
            sources.offer(pt);
        }
    }

    /**
     * Java 动态代理实现的InvocationHandler
     */
    class Handler implements InvocationHandler {
        Handler(T t) {
            target = t;
        }

        T target;

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            if (method.getName().equalsIgnoreCase(SimplePool.this.closemethod)) {
                SimplePool.this.sources.put((T) proxy);
                return null;
            }
            return method.invoke(target, args);
        }
    }

    /**
     * 获取可用资源数量
     *
     * @return
     */
    public int count() {
        return sources.size();
    }

    /**
     * 获取已放入池中的资源数量
     *
     * @return
     */
    public int size() {
        return this.origins.size();
    }
}