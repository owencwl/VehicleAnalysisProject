package com.umxwe.common.hbase.exception;

import java.util.Iterator;

/**
 * 创建多线程插入的异常
 */
public class HBaseInsertException extends Exception {
    public HBaseInsertException(String message) {
        super(message);
    }

    /**
     * 当一个异常被抛出的时候，可能有其他异常因为该异常而被抑制住，从而无法正常抛出。 这时可以通过addSuppressed方法把这些被抑制的方法记录下来。
     * 被抑制的异常会出现在抛出的异常的堆栈信息中，也可以通过getSuppressed方法来获取这些异常。 这样做的好处是不会丢失任何异常，方便开发人员进行调试。
     *
     * @param exceptions
     */
    public final synchronized void addSuppresseds(Iterable<Exception> exceptions) {
        if (exceptions != null) {
            Iterator<Exception> iterator = exceptions.iterator();
            while (iterator.hasNext()) {
                addSuppressed(iterator.next());
            }
        }
    }

}
