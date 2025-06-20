

//// File: BrokerInter.java
//import java.rmi.Remote;
//import java.rmi.RemoteException;
//import java.util.List;
//import java.util.Map;
//
///**
// * BrokerInter interface defines the remote methods for broker operations.
// */
//public interface BrokerInter extends Remote {
//    // Client-related methods
//    void createTopic(String topicId, String topicName, String publisherName) throws RemoteException;
//    void publishMessage(String topicId, String message, String publisherName) throws RemoteException;
//    void deleteTopic(String topicId, String publisherName) throws RemoteException;
//    int showSubscriberCount(String topicId) throws RemoteException;
//    void subscribe(String topicId, String subscriberName, SubscriberInterface subscriberCallback) throws RemoteException;
//    void unsubscribe(String topicId, String subscriberName) throws RemoteException;
//    List<Map<String, String>> listAllTopics() throws RemoteException;
//    List<Map<String, String>> showCurrentSubscriptions(String subscriberName) throws RemoteException;
//    void registerSubscriberCallback(String subscriberName, SubscriberInterface subscriberCallback) throws RemoteException;
//    void unregisterSubscriberCallback(String subscriberName) throws RemoteException;
//    void registerPublisher(String publisherName) throws RemoteException;
//    void deregisterPublisher(String publisherName) throws RemoteException;
//    void publisherHeartbeat(String publisherName) throws RemoteException;
//    
//    // Inter-Broker related methods
//    void addOtherBroker(BrokerInfo brokerInfo) throws RemoteException;
//    void registerRemoteTopic(String topicId, String topicName, String publisherName, String originatingBrokerId) throws RemoteException;
//    void notifyTopicDeletion(String topicId) throws RemoteException;
//    void forwardMessage(String topicId, String message, String publisherName) throws RemoteException;
//    String getBrokerID() throws RemoteException;
//}

//


import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * BrokerInter interface defines the remote methods for broker operations.
 */
public interface BrokerInter extends Remote {
    // Client-related methods
    void createTopic(String topicId, String topicName, String publisherName) throws RemoteException;
    void publishMessage(String topicId, String message, String publisherName) throws RemoteException;
    void deleteTopic(String topicId) throws RemoteException;
    int showSubscriberCount(String topicId) throws RemoteException;
    void subscribe(String topicId, String subscriberName, SubscriberInterface subscriberCallback) throws RemoteException;
    void unsubscribe(String topicId, String subscriberName) throws RemoteException;

    // Updated listAllTopics() method that accepts a set of visited brokers
    List<Map<String, String>> listAllTopics(Set<String> visitedBrokers) throws RemoteException;

    // Original listAllTopics() method (calls the new one internally)
    default List<Map<String, String>> listAllTopics() throws RemoteException {
        return listAllTopics(null);
    }

    List<Map<String, String>> showCurrentSubscriptions(String subscriberName) throws RemoteException;
    void registerSubscriberCallback(String subscriberName, SubscriberInterface subscriberCallback) throws RemoteException;
    void unregisterSubscriberCallback(String subscriberName) throws RemoteException;
    void registerPublisher(String publisherName) throws RemoteException;
    void deregisterPublisher(String publisherName) throws RemoteException;
    void publisherHeartbeat(String publisherName) throws RemoteException;

    // Inter-Broker related methods
    void addOtherBroker(BrokerInfo brokerInfo) throws RemoteException;
    void registerRemoteTopic(String topicId, String topicName, String publisherName, String originatingBrokerId) throws RemoteException;
    void notifyTopicDeletion(String topicId) throws RemoteException;
    void forwardMessage(String topicId, String message, String publisherName) throws RemoteException;
    String getBrokerID() throws RemoteException;
}


