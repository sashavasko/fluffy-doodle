package org.sv.pubsub;

import java.util.*;
import java.util.concurrent.*;

public class Topic implements AutoCloseable {
    String name;
    Set<Subscriber> subscribed = new CopyOnWriteArraySet<>();
    ExecutorService executor;
    final LinkedList<String> previousMessages = new LinkedList<>();

    public Topic(String name) {
        this.name = name;
        executor = Executors.newFixedThreadPool(5);
    }

    public boolean subscribe(Subscriber subscriber){
        if (subscriber != null && subscribed.add(subscriber)){
            List<DeliveryTask> tasks = new ArrayList<>();
            synchronized(previousMessages){
                Iterator<String> reverseIt = previousMessages.descendingIterator();
                while (reverseIt.hasNext()) {
                    String message = reverseIt.next();
                    tasks.add(new DeliveryTask(message, subscriber));
                }
                if (runTasks(tasks))
                    return subscribed.add(subscriber);
            }
        }
        return false;
    }

    public boolean unsubscribe(Subscriber subscriber){
        return subscribed.remove(subscriber);
    }

    public boolean runTasks(List<DeliveryTask> tasks){
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

    public boolean publish(String message) {
        List<DeliveryTask> tasks = new ArrayList<>();
        subscribed.forEach(s -> tasks.add(new DeliveryTask(message, s)));
        synchronized(previousMessages){
            previousMessages.addFirst(message);
        }
        return runTasks(tasks);
    }

    @Override
    public void close() throws Exception {
        executor.shutdown();
    }
}
