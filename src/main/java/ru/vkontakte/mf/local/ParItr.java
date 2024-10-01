package ru.vkontakte.mf.local;

import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * @author ezamyatin
 **/
public class ParItr {
    public static <A> void foreach(Iterator<A> iterator, int cpus, Consumer<A> fn) {
        LinkedBlockingQueue<A> inQueue = new LinkedBlockingQueue<A>(cpus * 5);
        AtomicLong totalCounter = new AtomicLong(0);
        Thread[] threads = new Thread[cpus];
        for (int i = 0; i < cpus; ++i) {
            threads[i] = new Thread((Runnable) () -> {
                while (true) {
                    A obj = null;
                    try {
                        obj = inQueue.take();
                    } catch (InterruptedException ex) {
                        break;
                    }
                    fn.accept(obj);
                    totalCounter.decrementAndGet();
                }
            });
        }
        Arrays.stream(threads).forEach(Thread::start);

        iterator.forEachRemaining(e -> {
            try {
                inQueue.put(e);
            } catch (InterruptedException ex) {
                throw new RuntimeException(ex);
            }
            totalCounter.incrementAndGet();
        });

        while (totalCounter.get() > 0) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ex) {
                throw new RuntimeException(ex);
            }
        }

        try {
            Arrays.stream(threads).forEach(Thread::interrupt);
            for (int i = 0; i < threads.length; ++i) {
                threads[i].join();
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        assert(totalCounter.get() == 0);
        assert(!iterator.hasNext());
    }
}
