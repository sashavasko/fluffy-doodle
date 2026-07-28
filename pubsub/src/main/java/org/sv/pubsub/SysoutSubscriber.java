package org.sv.pubsub;

public class SysoutSubscriber implements Subscriber{
    @Override
    public void onMessage(String message) {
        System.out.println(message);
    }
}
