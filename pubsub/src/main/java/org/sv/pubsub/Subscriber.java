package org.sv.pubsub;

public interface Subscriber {
    void onMessage(String message);
}
