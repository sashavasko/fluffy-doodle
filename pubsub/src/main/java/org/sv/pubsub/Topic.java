package org.sv.pubsub;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;

public class Topic implements AutoCloseable {
    String name;
    Set<Subscriber> subscribed = new CopyOnWriteArraySet<>();
    ExecutorService executor;

    public Topic(String name) {
        this.name = name;
        executor = Executors.newFixedThreadPool(5);
    }

    public boolean subscribe(Subscriber subscriber){
        return subscribed.add(subscriber);
    }

    public boolean unsubscribe(Subscriber subscriber){
        return subscribed.remove(subscriber);
    }

    public boolean publish(String message) {
        List<DeliveryTask> tasks = new ArrayList<>();
        subscribed.forEach(s -> tasks.add(new DeliveryTask(message, s)));
        try {
            List<Future<Subscriber>> futures = executor.invokeAll(tasks, 5, TimeUnit.SECONDS);
            for (Future<Subscriber> f : futures) {
                if (f.isCancelled()) {
                    System.out.println("Failed to deliver message to one of the subscribers");
                    return false;
                }
            }
        } catch (InterruptedException e) {
            return false;
        }
        return true;
    }

    @Override
    public void close() throws Exception {
        executor.shutdown();
    }
}
