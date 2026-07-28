package org.sv.pubsub;

import java.util.concurrent.Callable;

public class DeliveryTask implements Callable<Subscriber> {
    String message;
    Subscriber subscriber;

    public DeliveryTask(String message, Subscriber subscriber) {
        this.message = message;
        this.subscriber = subscriber;
    }

    @Override
    public Subscriber call() {
        subscriber.onMessage(message);
        return subscriber;
    }
}
