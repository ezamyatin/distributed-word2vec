package com.github.ezamyatin.logfac.local;

import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * @author ezamyatin
 */
public class ParItr {

    public static <A> void foreach(Iterator<A> iterator, int cpus, Consumer<A> fn) {
        LinkedBlockingQueue<A> inQueue = new LinkedBlockingQueue<>(cpus * 5);

        AtomicReference<Exception> error = new AtomicReference<>(null);
        AtomicBoolean end = new AtomicBoolean(false);
        CountDownLatch latch = new CountDownLatch(1);

        Thread[] threads = new Thread[cpus];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(() -> {
                try {
                    while (!end.get() || !inQueue.isEmpty()) {
                        fn.accept(inQueue.take());
                    }
                } catch (InterruptedException ignored){
                } catch (Exception e) {
                    error.set(e);
                } finally {
                    latch.countDown();
                }
            });
      }

        try {
            Arrays.stream(threads).forEach(Thread::start);
            iterator.forEachRemaining(e -> {
                boolean ok = false;
                while (!ok) {
                    if (error.get() != null) {
                        throw new RuntimeException(error.get());
                    }

                    try {
                        ok = inQueue.offer(e, 1, TimeUnit.SECONDS);
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }
                }
            });

            end.set(true);
            if (!inQueue.isEmpty()) {
                latch.await();
            }

            if (error.get() != null) {
                throw error.get();
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            Arrays.stream(threads).forEach(Thread::interrupt);
            Arrays.stream(threads).forEach(e -> {
                try {
                    e.join();
                } catch (InterruptedException ex) {
                    throw new RuntimeException(ex);
                }
            });
        }
    }
}
