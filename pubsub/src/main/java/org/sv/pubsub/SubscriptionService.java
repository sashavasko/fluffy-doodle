package org.sv.pubsub;

import java.util.HashMap;
import java.util.Map;

public class SubscriptionService implements AutoCloseable{
    final Map<String,Topic> topics = new HashMap<>();
    boolean closed = false;

    public boolean subscribe(String topicName, Subscriber subscriber) {
        Topic topic = topics.get(topicName);
        if (topic != null)
            return topic.subscribe(subscriber);
        return false;
    }
    public boolean unsubscribe(String topicName, Subscriber subscriber) {
        Topic topic = topics.get(topicName);
        if (topic != null)
            return topic.unsubscribe(subscriber);
        return false;
    }
    public boolean publish(String topicName, String message) {
        Topic topic = topics.get(topicName);
        if (topic != null)
            return topic.publish(message);
        return true;
    }
    public boolean addTopic(String topicName){
        if (topics.containsKey(topicName))
            return false;
        synchronized(topics) {
            if(closed)
                return false;
            Topic topic = topics.get(topicName);
            if (topic != null)
                return false;
            topics.put(topicName, new Topic(topicName));
        }
        return true;
    }

    @Override
    public void close() throws Exception {
        synchronized(topics) {
            for (Topic t : topics.values()) {
                if (t != null)
                    t.close();
            }
            closed = true;
        }
    }
}
