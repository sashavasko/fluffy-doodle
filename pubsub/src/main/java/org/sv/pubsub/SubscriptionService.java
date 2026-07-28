package org.sv.pubsub;

import java.util.HashMap;
import java.util.Map;

public class SubscriptionService implements AutoCloseable{
    Map<String,Topic> topics = new HashMap<>();

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
        Topic topic = topics.get(topicName);
        if (topic != null)
            return false;
        topics.put(topicName, new Topic(topicName));
        return true;
    }


    @Override
    public void close() throws Exception {
        for (Topic t : topics.values()){
            if (t != null)
                t.close();
        }
    }
}
