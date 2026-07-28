package org.sv.pubsub;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SubscriptionServiceTest {

    public static class TestSubscriber implements Subscriber{
        String name;
        List<String> messagesReceived = new ArrayList<>();

        public TestSubscriber(String name) {
            this.name = name;
        }

        @Override
        public void onMessage(String message) {
            messagesReceived.add(message);
            System.out.println(name + ":" + message);
        }

        public List<String> getMessagesReceived() {
            return messagesReceived;
        }

        @Override
        public String toString() {
            return "TestSubscriber{" +
                    "name='" + name + '\'' +
                    ", messagesReceived=" + messagesReceived +
                    '}';
        }
    }

    public static class BlockinSubscriber extends TestSubscriber{

        public BlockinSubscriber(String name) {
            super(name);
        }

        @Override
        public void onMessage(String message) {
            super.onMessage(message);
            while(true);
        }
    }

    @Test
    void coupleSysouts() throws Exception {
        TestSubscriber test1 = new TestSubscriber("test1");
        TestSubscriber test2 = new TestSubscriber("test2");
        try(SubscriptionService service = new SubscriptionService()){
            service.addTopic("foo");
            service.subscribe("foo", test1);
            service.subscribe("foo", test2);

            assertTrue(service.publish("foo", "message1"));
            assertTrue(service.publish("foo", "message2"));
        }

        assertEquals(2, test1.getMessagesReceived().size());
        assertEquals(2, test2.getMessagesReceived().size());
    }

    @Test
    void blocking() throws Exception {
        TestSubscriber test1 = new TestSubscriber("test1");
        TestSubscriber test2 = new BlockinSubscriber("test2");
        try(SubscriptionService service = new SubscriptionService()){
            service.addTopic("foo");
            service.subscribe("foo", test1);
            service.subscribe("foo", test2);

            assertFalse(service.publish("foo", "message1"));
            assertFalse(service.publish("foo", "message2"));
        }

        assertEquals(2, test1.getMessagesReceived().size());
        assertEquals(2, test2.getMessagesReceived().size());
    }

}