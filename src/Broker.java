// File: Broker.java
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.ConnectException;
import java.rmi.ConnectIOException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.net.BindException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Broker class implements the BrokerInter interface and manages topics, subscribers, and inter-broker communications.
 * Brokers are uniquely identified by brokerID, which is auto-generated based on IP address and port numbers.
 */
public class Broker extends UnicastRemoteObject implements BrokerInter {

    private static final long serialVersionUID = 1L;

    // Data structures to store topics, subscribers, and their relationships
    private final ConcurrentMap<String, Topic> topics; // Maps topicId to Topic
    private final ConcurrentMap<String, CopyOnWriteArraySet<String>> topicSubscribers; // Maps topicId to set of subscriber names
    private final ConcurrentMap<String, CopyOnWriteArraySet<String>> subscriberTopics; // Maps subscriberName to set of topicIds
    private final ConcurrentMap<String, SubscriberInterface> subscriberCallbacks; // Maps subscriberName to their callback objects

    // Publisher management
    private final Set<String> activePublishers; // Track active publishers
    private final ConcurrentMap<String, Long> publisherHeartbeats; // Publisher heartbeat timestamps

    // Broker identification
    private final String brokerID; // Unique identifier for the broker
    private final String ipAddress;
    private final int clientPort;      // Port for communication with publishers/subscribers
    private final int interBrokerPort; // Port for inter-broker communication

    // Directory Service reference
    private DirectoryServiceInter directoryService;

    // Map of other brokers
    private final ConcurrentMap<String, BrokerInter> otherBrokers; // Maps brokerID to BrokerInter

    // Binding names and URLs for RMI
    private final String clientBindingName;
    private final String interBrokerBindingName;
    private final String clientURL;
    private final String interBrokerURL;

    // Lock to manage broker addition to prevent race conditions
    private final ReentrantLock brokerAdditionLock;

    /**
     * Constructor for Broker.
     *
     * @param ipAddress         IP address of the broker.
     * @param clientPort        Communication port for clients (publishers/subscribers).
     * @param interBrokerPort   Communication port for inter-broker communication.
     * @throws RemoteException If an RMI error occurs.
     */
    public Broker(String ipAddress, int clientPort, int interBrokerPort) throws RemoteException {
        // Automatically exports the object on the interBrokerPort for inter-broker communication
        super(interBrokerPort);
        this.ipAddress = ipAddress;
        this.clientPort = clientPort;
        this.interBrokerPort = interBrokerPort;
        this.brokerID = generateBrokerID(ipAddress, clientPort, interBrokerPort);
        this.clientBindingName = "brokerClient_" + brokerID;       // e.g., "brokerClient_localhost:1080:1098"
        this.interBrokerBindingName = "brokerInter_" + brokerID;  // e.g., "brokerInter_localhost:1080:1098"
        this.clientURL = "rmi://" + ipAddress + ":" + clientPort + "/" + clientBindingName;
        this.interBrokerURL = "rmi://" + ipAddress + ":" + interBrokerPort + "/" + interBrokerBindingName;

        // Initialize data structures with thread-safe collections
        topics = new ConcurrentHashMap<>();
        topicSubscribers = new ConcurrentHashMap<>();
        subscriberTopics = new ConcurrentHashMap<>();
        subscriberCallbacks = new ConcurrentHashMap<>();
        otherBrokers = new ConcurrentHashMap<>();
        activePublishers = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
        publisherHeartbeats = new ConcurrentHashMap<>();

        brokerAdditionLock = new ReentrantLock(); // Initialize the lock

        try {
            // Step 1: Create RMI Registry for Client Communication
            try {
                LocateRegistry.createRegistry(clientPort);
                System.out.println("[INFO] Client RMI registry created on port " + clientPort + ".");
            } catch (RemoteException e) {
                // RMI registry already exists, proceed without error
                System.out.println("[INFO] Client RMI registry already exists on port " + clientPort + ".");
            }

            // Step 2: Bind broker for Client Communication
            Naming.rebind(clientURL, this);
            System.out.println("[INFO] Broker '" + brokerID + "' bound for client communication at " + clientURL + ".");

            // Step 3: Create RMI Registry for Inter-Broker Communication
            try {
                LocateRegistry.createRegistry(interBrokerPort);
                System.out.println("[INFO] Inter-Broker RMI registry created on port " + interBrokerPort + ".");
            } catch (RemoteException e) {
                // RMI registry already exists, proceed without error
                System.out.println("[INFO] Inter-Broker RMI registry already exists on port " + interBrokerPort + ".");
            }

            // Step 4: Bind broker for Inter-Broker Communication
            Naming.rebind(interBrokerURL, this);
            System.out.println("[INFO] Broker '" + brokerID + "' bound for inter-broker communication at " + interBrokerURL + ".");

            // Step 5: Connect to the Directory Service on the default RMI registry port (e.g., 1099)
            String directoryServiceURL = "rmi://localhost:1099/DirectoryService";
            directoryService = (DirectoryServiceInter) Naming.lookup(directoryServiceURL);
            System.out.println("[INFO] Connected to Directory Service at " + directoryServiceURL + ".");

            // Step 6: Register the broker with the Directory Service using both ports
            directoryService.registerBroker(brokerID, ipAddress, clientPort, interBrokerPort);
            System.out.println("[INFO] Broker '" + brokerID + "' registered with Directory Service at " + ipAddress + ":" + clientPort + " and inter-broker port " + interBrokerPort + ".");

            // Step 7: Get the list of other brokers and establish connections
            List<BrokerInfo> brokerList = directoryService.getActiveBrokers();
            for (BrokerInfo brokerInfo : brokerList) {
                if (!brokerInfo.getBrokerID().equals(this.brokerID)) {
                    addOtherBrokerSafely(brokerInfo);
                }
            }

            // Step 8: Start the heartbeat checker in a separate thread
            startHeartbeatChecker();

            // Step 9: Start periodic broker discovery
            startPeriodicBrokerDiscovery();

            // Step 10: Register shutdown hook for graceful shutdown
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    Naming.unbind(clientURL);
                    Naming.unbind(interBrokerURL);
                    UnicastRemoteObject.unexportObject(this, true);
                    directoryService.deregisterBroker(brokerID); // Implemented in Directory Service
                    System.out.println("[INFO] Broker '" + brokerID + "' unbound and shut down gracefully.");
                } catch (Exception e) {
                    System.err.println("[ERROR] Failed to unbind Broker '" + brokerID + "'.");
                    e.printStackTrace();
                }
            }));

        } catch (Exception e) {
            System.err.println("[FATAL] Failed to initialize Broker '" + brokerID + "'.");
            e.printStackTrace();
            throw new RemoteException("Failed to initialize Broker '" + brokerID + "'.", e);
        }
    }

    /**
     * Generates a unique brokerID based on IP address and port numbers.
     *
     * @param ipAddress         IP address of the broker.
     * @param clientPort        Client communication port.
     * @param interBrokerPort   Inter-broker communication port.
     * @return Unique brokerID as a String.
     */
    private String generateBrokerID(String ipAddress, int clientPort, int interBrokerPort) {
        return ipAddress + ":" + clientPort + ":" + interBrokerPort;
    }

    /**
     * Inner class to represent a Topic.
     */
    public static class Topic implements java.io.Serializable {
        private static final long serialVersionUID = 1L;
        String topicId;
        String topicName;
        String publisherName;
        List<String> messages;

        public Topic(String topicId, String topicName, String publisherName) {
            this.topicId = topicId;
            this.topicName = topicName;
            this.publisherName = publisherName;
            this.messages = new CopyOnWriteArrayList<>();
        }
    }

    // ===================== Implementation of BrokerInter Methods =====================

    /**
     * Creates a new topic.
     *
     * @param topicId        Unique identifier for the topic.
     * @param topicName      Name of the topic.
     * @param publisherName  Name of the publisher creating the topic.
     * @throws RemoteException If the topic ID already exists or other RMI issues occur.
     */
    @Override
    public void createTopic(String topicId, String topicName, String publisherName) throws RemoteException {
        Objects.requireNonNull(topicId, "topicId cannot be null");
        Objects.requireNonNull(topicName, "topicName cannot be null");
        Objects.requireNonNull(publisherName, "publisherName cannot be null");
        if (topics.containsKey(topicId)) {
            throw new RemoteException("Topic ID already exists.");
        }

        // Use putIfAbsent to avoid race conditions
        Topic newTopic = new Topic(topicId, topicName, publisherName);
        if (topics.putIfAbsent(topicId, newTopic) != null) {
            throw new RemoteException("Topic ID already exists.");
        }
        System.out.println("[INFO] Created new topic: '" + topicName + "' with ID: " + topicId + ".");

        // Assign topic to this broker in Directory Service
        directoryService.assignTopicToBroker(topicId, brokerID);
        System.out.println("[INFO] Assigned topic ID " + topicId + " to broker '" + brokerID + "' in Directory Service.");

        // Notify other brokers about the new topic via inter-broker communication
        for (BrokerInter broker : otherBrokers.values()) {
            try {
                broker.registerRemoteTopic(topicId, topicName, publisherName, this.brokerID);
            } catch (RemoteException e) {
                System.err.println("[WARN] Failed to notify broker about new topic: '" + topicName + "'.");
                e.printStackTrace();
            }
        }
    }

    /**
     * Publishes a message to a topic.
     *
     * @param topicId        ID of the topic.
     * @param message        Message to be published.
     * @param publisherName  Name of the publisher.
     * @throws RemoteException If the topic does not exist or publisher name mismatch occurs.
     */
    @Override
    public void publishMessage(String topicId, String message, String publisherName) throws RemoteException {
        Objects.requireNonNull(topicId, "topicId cannot be null");
        Objects.requireNonNull(message, "message cannot be null");
        Objects.requireNonNull(publisherName, "publisherName cannot be null");

        Topic topic = topics.get(topicId);
        if (topic == null) {
            throw new RemoteException("Topic not found.");
        }
        if (!topic.publisherName.equals(publisherName)) {
            throw new RemoteException("Publisher name does not match topic's publisher.");
        }

        // Add message to topic
        topic.messages.add(message);
        System.out.println("[INFO] Published message to topic '" + topic.topicName + "': " + message);

        // Notify local subscribers
        Set<String> subscribers = topicSubscribers.get(topicId);
        if (subscribers != null && !subscribers.isEmpty()) {
            for (String subscriberName : subscribers) {
                SubscriberInterface subscriberCallback = subscriberCallbacks.get(subscriberName);
                if (subscriberCallback != null) {
                    try {
                        subscriberCallback.receiveMessage(topicId, topic.topicName, message, publisherName);
                        System.out.println("[INFO] Notified subscriber '" + subscriberName + "' of new message on topic '" + topic.topicName + "'.");
                    } catch (Exception e) {
                        // Handle subscriber failure
                        System.err.println("[WARN] Failed to notify subscriber '" + subscriberName + "'. Unsubscribing.");
                        subscriberCallbacks.remove(subscriberName);
                        unsubscribe(topicId, subscriberName);
                    }
                }
            }
        }

        // Forward the message to other brokers via inter-broker communication
        for (BrokerInter broker : otherBrokers.values()) {
            try {
                broker.forwardMessage(topicId, message, publisherName);
            } catch (RemoteException e) {
                System.err.println("[WARN] Failed to forward message to broker '" + broker.getBrokerID() + "'.");
                e.printStackTrace();
                // Optionally, handle broker disconnection here
            }
        }
    }

    /**
     * Deletes a topic.
     *
     * @param topicId        ID of the topic.
     * @param publisherName  Name of the publisher attempting deletion.
     * @throws RemoteException If the topic does not exist or publisher name mismatch occurs.
     */
    @Override
    public void deleteTopic(String topicId) throws RemoteException {
        Objects.requireNonNull(topicId, "topicId cannot be null");

        Topic topic = topics.get(topicId);
        if (topic == null) {
            throw new RemoteException("Topic not found.");
        }
        

        // Remove the topic atomically
        if (topics.remove(topicId) == null) {
            throw new RemoteException("Failed to remove topic. It may have been already deleted.");
        }
        System.out.println("[INFO] Deleted topic: '" + topic.topicName + "' with ID: " + topicId + ".");

        // Remove subscribers from the topic
        Set<String> subscribers = topicSubscribers.remove(topicId);
        if (subscribers != null) {
            for (String subscriberName : subscribers) {
                SubscriberInterface subscriberCallback = subscriberCallbacks.get(subscriberName);
                if (subscriberCallback != null) {
                    try {
                        subscriberCallback.notifyTopicDeletion(topicId, topic.topicName);
                        System.out.println("[INFO] Notified subscriber '" + subscriberName + "' about deletion of topic '" + topic.topicName + "'.");
                    } catch (Exception e) {
                        // Handle subscriber failure
                        System.err.println("[WARN] Failed to notify subscriber '" + subscriberName + "'. Unsubscribing.");
                        subscriberCallbacks.remove(subscriberName);
                        unsubscribe(topicId, subscriberName);
                    }
                }
                // Remove topic from subscriber's list
                CopyOnWriteArraySet<String> topicsSet = subscriberTopics.get(subscriberName);
                if (topicsSet != null) {
                    topicsSet.remove(topicId);
                    if (topicsSet.isEmpty()) {
                        subscriberTopics.remove(subscriberName);
                    }
                }
            }
        }

        // Notify other brokers about the topic deletion via inter-broker communication
        for (BrokerInter broker : otherBrokers.values()) {
            try {
                broker.notifyTopicDeletion(topicId);
            } catch (RemoteException e) {
                System.err.println("[WARN] Failed to notify broker '" + broker.getBrokerID() + "' about topic deletion.");
                e.printStackTrace();
                // Optionally, handle broker disconnection here
            }
        }

        try {
            directoryService.assignTopicToBroker(topicId, null);
            System.out.println("[INFO] Removed topic ID " + topicId + " from broker assignment in Directory Service.");
        } catch (RemoteException e) {
            System.err.println("[WARN] Failed to update Directory Service for topic ID " + topicId + ". It might have been already unassigned or deleted.");
        }
    }

    /**
     * Shows the number of subscribers for a given topic.
     *
     * @param topicId ID of the topic.
     * @return Number of subscribers.
     * @throws RemoteException If an RMI error occurs.
     */
    @Override
    public int showSubscriberCount(String topicId) throws RemoteException {
        Objects.requireNonNull(topicId, "topicId cannot be null");
        Set<String> subscribers = topicSubscribers.get(topicId);
        int count = (subscribers != null) ? subscribers.size() : 0;
        System.out.println("[INFO] Subscriber count for topic ID " + topicId + ": " + count);
        return count;
    }

    /**
     * Subscribes a subscriber to a topic.
     *
     * @param topicId            ID of the topic.
     * @param subscriberName     Name of the subscriber.
     * @param subscriberCallback Callback interface for the subscriber.
     * @throws RemoteException If the topic does not exist or other RMI issues occur.
     */
    @Override
    public void subscribe(String topicId, String subscriberName, SubscriberInterface subscriberCallback) throws RemoteException {
        Objects.requireNonNull(topicId, "topicId cannot be null");
        Objects.requireNonNull(subscriberName, "subscriberName cannot be null");
        Objects.requireNonNull(subscriberCallback, "subscriberCallback cannot be null");

        // Determine which broker manages the topic
        String managingBrokerId = directoryService.getBrokerForTopic(topicId);
        System.out.println("[DEBUG] Subscription request for topic ID: " + topicId + " managed by broker: " + managingBrokerId);
        if (managingBrokerId == null) {
            throw new RemoteException("No broker found managing topic ID: " + topicId);
        }

        if (managingBrokerId.equals(this.brokerID)) {
            // This broker manages the topic; proceed with subscription
            Topic topic = topics.get(topicId);
            if (topic == null) {
                throw new RemoteException("Topic not found.");
            }

            // Add subscriber to the topic's subscriber set
            topicSubscribers.computeIfAbsent(topicId, k -> new CopyOnWriteArraySet<>()).add(subscriberName);

            // Add topic to the subscriber's topic set
            subscriberTopics.computeIfAbsent(subscriberName, k -> new CopyOnWriteArraySet<>()).add(topicId);

            // Register the subscriber's callback
            subscriberCallbacks.put(subscriberName, subscriberCallback);
            System.out.println("[INFO] Subscriber '" + subscriberName + "' subscribed to topic '" + topic.topicName + "'.");
        } else {
            // Delegate subscription to the responsible broker via inter-broker communication
            BrokerInter managingBroker = otherBrokers.get(managingBrokerId);
            if (managingBroker != null) {
                try {
                    managingBroker.subscribe(topicId, subscriberName, subscriberCallback);
                    System.out.println("[INFO] Delegated subscription of '" + subscriberName + "' to broker '" + managingBrokerId + "'.");
                } catch (RemoteException e) {
                    System.err.println("[ERROR] Failed to delegate subscription to broker '" + managingBrokerId + "'.");
                    e.printStackTrace();
                    throw new RemoteException("Failed to delegate subscription to broker '" + managingBrokerId + "'.", e);
                }
            } else {
                throw new RemoteException("Responsible broker '" + managingBrokerId + "' not found.");
            }
        }
    }

    /**
     * Unsubscribes a subscriber from a topic.
     *
     * @param topicId        ID of the topic.
     * @param subscriberName Name of the subscriber.
     * @throws RemoteException If an RMI error occurs.
     */
    @Override
    public void unsubscribe(String topicId, String subscriberName) throws RemoteException {
        Objects.requireNonNull(topicId, "topicId cannot be null");
        Objects.requireNonNull(subscriberName, "subscriberName cannot be null");

        // Determine which broker manages the topic
        String managingBrokerId = directoryService.getBrokerForTopic(topicId);
        if (managingBrokerId == null) {
            throw new RemoteException("No broker found managing topic ID: " + topicId);
        }

        if (managingBrokerId.equals(this.brokerID)) {
            // This broker manages the topic; proceed with unsubscription
            Set<String> subscribers = topicSubscribers.get(topicId);
            if (subscribers != null) {
                subscribers.remove(subscriberName);
                System.out.println("[INFO] Subscriber '" + subscriberName + "' unsubscribed from topic ID " + topicId + ".");
            }

            // Remove topic from subscriber's topic set
            Set<String> topicsSet = subscriberTopics.get(subscriberName);
            if (topicsSet != null) {
                topicsSet.remove(topicId);
                if (topicsSet.isEmpty()) {
                    subscriberTopics.remove(subscriberName);
                }
            }

            // Remove subscriber's callback if no longer subscribed to any topics
            if (subscriberTopics.get(subscriberName) == null) {
                subscriberCallbacks.remove(subscriberName);
            }
        } else {
            // Delegate unsubscription to the responsible broker via inter-broker communication
            BrokerInter managingBroker = otherBrokers.get(managingBrokerId);
            if (managingBroker != null) {
                managingBroker.unsubscribe(topicId, subscriberName);
                System.out.println("[INFO] Delegated unsubscription of '" + subscriberName + "' to broker '" + managingBrokerId + "'.");
            } else {
                throw new RemoteException("Responsible broker '" + managingBrokerId + "' not found.");
            }
        }
    }

    /**
     * Lists all available topics across all brokers.
     *
     * @return List of topics with their details.
     * @throws RemoteException If an RMI error occurs.
     */
    @Override
    public List<Map<String, String>> listAllTopics(Set<String> visitedBrokers) throws RemoteException {
        if (visitedBrokers == null) {
            visitedBrokers = new HashSet<>();
        }

        if (!visitedBrokers.add(this.brokerID)) {
            // Broker has already been visited, skip to avoid infinite recursion
            return Collections.emptyList();
        }

        List<Map<String, String>> topicList = new ArrayList<>();
        
        // Add local topics
        for (Topic topic : topics.values()) {
            Map<String, String> topicInfo = new HashMap<>();
            topicInfo.put("topicId", topic.topicId);
            topicInfo.put("topicName", topic.topicName);
            topicInfo.put("publisherName", topic.publisherName);
            topicList.add(topicInfo);
        }

        // Add remote topics from other brokers
        for (BrokerInter broker : otherBrokers.values()) {
            try {
                List<Map<String, String>> remoteTopics = broker.listAllTopics(visitedBrokers);
                topicList.addAll(remoteTopics);
            } catch (RemoteException e) {
                System.err.println("[WARN] Failed to retrieve topics from broker '" + broker.getBrokerID() + "'.");
                e.printStackTrace();
            }
        }

        System.out.println("[INFO] Listing all topics. Total: " + topicList.size() + ".");
        return topicList;
    }


    /**
     * Shows current subscriptions for a subscriber.
     *
     * @param subscriberName Name of the subscriber.
     * @return List of topics the subscriber is subscribed to.
     * @throws RemoteException If an RMI error occurs.
     */
    @Override
    public List<Map<String, String>> showCurrentSubscriptions(String subscriberName) throws RemoteException {
        Objects.requireNonNull(subscriberName, "subscriberName cannot be null");
        Set<String> subscribedTopics = subscriberTopics.get(subscriberName);
        List<Map<String, String>> subscriptionList = new ArrayList<>();
        if (subscribedTopics != null) {
            for (String topicId : subscribedTopics) {
                String managingBrokerId = directoryService.getBrokerForTopic(topicId);
                if (managingBrokerId == null) {
                    continue;
                }

                if (managingBrokerId.equals(this.brokerID)) {
                    Topic topic = topics.get(topicId);
                    if (topic != null) {
                        Map<String, String> topicInfo = new HashMap<>();
                        topicInfo.put("topicId", topic.topicId);
                        topicInfo.put("topicName", topic.topicName);
                        subscriptionList.add(topicInfo);
                    }
                } else {
                    BrokerInter managingBroker = otherBrokers.get(managingBrokerId);
                    if (managingBroker != null) {
                        try {
                            List<Map<String, String>> remoteSubscriptions = managingBroker.showCurrentSubscriptions(subscriberName);
                            subscriptionList.addAll(remoteSubscriptions);
                        } catch (RemoteException e) {
                            System.err.println("[WARN] Failed to retrieve subscriptions from broker '" + managingBrokerId + "'.");
                            e.printStackTrace();
                        }
                    }
                }
            }
        }
        System.out.println("[INFO] Showing current subscriptions for subscriber '" + subscriberName + "'. Total: " + subscriptionList.size() + ".");
        return subscriptionList;
    }

    /**
     * Registers a remote topic from another broker.
     *
     * @param topicId              ID of the topic.
     * @param topicName            Name of the topic.
     * @param publisherName        Name of the publisher.
     * @param originatingBrokerId  ID of the originating broker.
     * @throws RemoteException If an RMI error occurs.
     */

//    @Override
//    public void registerRemoteTopic(String topicId, String topicName, String publisherName, String originatingBrokerId) throws RemoteException {
//        Objects.requireNonNull(topicId, "topicId cannot be null");
//        Objects.requireNonNull(topicName, "topicName cannot be null");
//        Objects.requireNonNull(publisherName, "publisherName cannot be null");
//        Objects.requireNonNull(originatingBrokerId, "originatingBrokerId cannot be null");
//
//        // Check if the topic already exists before registering it
//        if (topics.containsKey(topicId)) {
//            System.out.println("[DEBUG] Topic ID " + topicId + " already exists. Skipping registration from broker: " + originatingBrokerId);
//            return;
//        }
//
//        // Register the new remote topic
//        Topic remoteTopic = new Topic(topicId, topicName, publisherName);
//        topics.putIfAbsent(topicId, remoteTopic);
//        System.out.println("[INFO] Registered remote topic '" + topicName + "' with ID " + topicId + " from broker '" + originatingBrokerId + "'.");
//    }
    private final Set<String> processedTopics = ConcurrentHashMap.newKeySet(); // Set to track processed topics

    public void registerRemoteTopic(String topicId, String topicName, String publisherName, String originatingBrokerId) throws RemoteException {
        Objects.requireNonNull(topicId, "topicId cannot be null");
        Objects.requireNonNull(topicName, "topicName cannot be null");
        Objects.requireNonNull(publisherName, "publisherName cannot be null");
        Objects.requireNonNull(originatingBrokerId, "originatingBrokerId cannot be null");

        // Unique key for tracking topics across brokers
        String topicKey = topicId + "|" + originatingBrokerId;

        // Check if this topic has already been processed to avoid cycles
        if (!processedTopics.add(topicKey)) {
            System.out.println("[DEBUG] Topic ID " + topicId + " already processed from broker: " + originatingBrokerId + ". Skipping registration.");
            return;
        }

        // Register the new remote topic if not already present locally
        if (!topics.containsKey(topicId)) {
            Topic remoteTopic = new Topic(topicId, topicName, publisherName);
            topics.putIfAbsent(topicId, remoteTopic);
            System.out.println("[INFO] Registered remote topic '" + topicName + "' with ID " + topicId + " from broker '" + originatingBrokerId + "'.");
        }
    }



    /**
     * Notifies the broker about the deletion of a topic from another broker.
     *
     * @param topicId ID of the topic to delete.
     * @throws RemoteException If an RMI error occurs.
     */
    @Override
    public void notifyTopicDeletion(String topicId) throws RemoteException {
        Objects.requireNonNull(topicId, "topicId cannot be null");

        Topic topic = topics.remove(topicId);
        if (topic != null) {
            // Remove subscribers from the topic
            Set<String> subscribers = topicSubscribers.remove(topicId);
            if (subscribers != null) {
                for (String subscriberName : subscribers) {
                    SubscriberInterface subscriberCallback = subscriberCallbacks.get(subscriberName);
                    if (subscriberCallback != null) {
                        try {
                            subscriberCallback.notifyTopicDeletion(topicId, topic.topicName);
                            System.out.println("[INFO] Notified subscriber '" + subscriberName + "' about deletion of remote topic '" + topic.topicName + "'.");
                        } catch (Exception e) {
                            // Handle subscriber failure
                            System.err.println("[WARN] Failed to notify subscriber '" + subscriberName + "'. Unsubscribing.");
                            subscriberCallbacks.remove(subscriberName);
                            unsubscribe(topicId, subscriberName);
                        }
                    }
                    // Remove topic from subscriber's topic set
                    Set<String> topicsSet = subscriberTopics.get(subscriberName);
                    if (topicsSet != null) {
                        topicsSet.remove(topicId);
                        if (topicsSet.isEmpty()) {
                            subscriberTopics.remove(subscriberName);
                        }
                    }
                }
            }
            System.out.println("[INFO] Deleted remote topic with ID " + topicId + ".");
        }
    }

    /**
     * Forwards a message from a publisher to local subscribers.
     *
     * @param topicId        ID of the topic.
     * @param message        Message to forward.
     * @param publisherName  Name of the publisher.
     * @throws RemoteException If an RMI error occurs.
     */
    @Override
    public void forwardMessage(String topicId, String message, String publisherName) throws RemoteException {
        Objects.requireNonNull(topicId, "topicId cannot be null");
        Objects.requireNonNull(message, "message cannot be null");
        Objects.requireNonNull(publisherName, "publisherName cannot be null");

        Topic topic = topics.get(topicId);
        if (topic != null && topic.publisherName.equals(publisherName)) {
            // Add message to topic
            topic.messages.add(message);
            System.out.println("[INFO] Forwarded message to topic '" + topic.topicName + "': " + message);

            // Notify local subscribers
            Set<String> subscribers = topicSubscribers.get(topicId);
            if (subscribers != null && !subscribers.isEmpty()) {
                for (String subscriberName : subscribers) {
                    SubscriberInterface subscriberCallback = subscriberCallbacks.get(subscriberName);
                    if (subscriberCallback != null) {
                        try {
                            subscriberCallback.receiveMessage(topicId, topic.topicName, message, publisherName);
                            System.out.println("[INFO] Notified subscriber '" + subscriberName + "' of forwarded message on topic '" + topic.topicName + "'.");
                        } catch (Exception e) {
                            // Handle subscriber failure
                            System.err.println("[WARN] Failed to notify subscriber '" + subscriberName + "'. Unsubscribing.");
                            subscriberCallbacks.remove(subscriberName);
                            unsubscribe(topicId, subscriberName);
                        }
                    }
                }
            }
        }
    }

    /**
     * Registers a subscriber's callback interface.
     *
     * @param subscriberName     Name of the subscriber.
     * @param subscriberCallback Callback interface of the subscriber.
     * @throws RemoteException If an RMI error occurs.
     */
    @Override
    public void registerSubscriberCallback(String subscriberName, SubscriberInterface subscriberCallback) throws RemoteException {
        Objects.requireNonNull(subscriberName, "subscriberName cannot be null");
        Objects.requireNonNull(subscriberCallback, "subscriberCallback cannot be null");

        subscriberCallbacks.put(subscriberName, subscriberCallback);
        System.out.println("[INFO] Registered callback for subscriber '" + subscriberName + "'.");
    }

    /**
     * Unregisters a subscriber's callback interface.
     *
     * @param subscriberName Name of the subscriber.
     * @throws RemoteException If an RMI error occurs.
     */
    @Override
    public void unregisterSubscriberCallback(String subscriberName) throws RemoteException {
        Objects.requireNonNull(subscriberName, "subscriberName cannot be null");

        subscriberCallbacks.remove(subscriberName);
        // Also remove from all subscribed topics
        Set<String> topicsSet = subscriberTopics.remove(subscriberName);
        if (topicsSet != null) {
            for (String topicId : topicsSet) {
                Set<String> subscribers = topicSubscribers.get(topicId);
                if (subscribers != null) {
                    subscribers.remove(subscriberName);
                }
            }
        }
        System.out.println("[INFO] Unregistered callback for subscriber '" + subscriberName + "'.");
    }

    /**
     * Registers a publisher.
     *
     * @param publisherName Name of the publisher to register.
     * @throws RemoteException If an RMI error occurs.
     */
    @Override
    public void registerPublisher(String publisherName) throws RemoteException {
        Objects.requireNonNull(publisherName, "publisherName cannot be null");
        System.out.println("[DEBUG] Attempting to register Publisher '" + publisherName + "'.");

        if (activePublishers.contains(publisherName)) {
            System.out.println("[WARN] Publisher '" + publisherName + "' is already registered.");
        } else {
            activePublishers.add(publisherName);
            System.out.println("[INFO] Publisher '" + publisherName + "' registered successfully.");
        }
    }

    /**
     * Deregisters a publisher.
     *
     * @param publisherName Name of the publisher to deregister.
     * @throws RemoteException If an RMI error occurs.
     */
    @Override
    public void deregisterPublisher(String publisherName) throws RemoteException {
        Objects.requireNonNull(publisherName, "publisherName cannot be null");
        System.out.println("[DEBUG] Attempting to deregister Publisher '" + publisherName + "'.");

        if (activePublishers.remove(publisherName)) {
            System.out.println("[INFO] Publisher '" + publisherName + "' deregistered successfully.");
            // Optionally, handle cleanup related to the publisher
        } else {
            System.out.println("[WARN] Publisher '" + publisherName + "' is not registered.");
        }
    }

    /**
     * Receives a heartbeat signal from a publisher.
     *
     * @param publisherName Name of the publisher sending the heartbeat.
     * @throws RemoteException If an RMI error occurs.
     */
    @Override
    public void publisherHeartbeat(String publisherName) throws RemoteException {
        Objects.requireNonNull(publisherName, "publisherName cannot be null");
        publisherHeartbeats.put(publisherName, System.currentTimeMillis());
        System.out.println("[INFO] Received heartbeat from publisher '" + publisherName + "'.");
    }

    /**
     * Adds another broker to the network safely to prevent race conditions.
     *
     * @param brokerInfo Information about the broker to add.
     * @throws RemoteException If an RMI error occurs.
     */
    private void addOtherBrokerSafely(BrokerInfo brokerInfo) throws RemoteException {
        if (brokerInfo.getBrokerID().equals(this.brokerID)) {
            System.out.println("[INFO] Broker '" + brokerInfo.getBrokerID() + "' is itself. Skipping addition.");
            return;
        }

        // Log the broker details before locking
        System.out.println("[DEBUG] Preparing to connect to broker: " + brokerInfo);

        brokerAdditionLock.lock(); // Lock to prevent concurrent additions
        try {
            System.out.println("[DEBUG] Acquired lock. Checking if broker '" + brokerInfo.getBrokerID() + "' is already connected.");

            if (otherBrokers.containsKey(brokerInfo.getBrokerID())) {
                System.out.println("[INFO] Broker '" + brokerInfo.getBrokerID() + "' is already connected. Skipping addition.");
                return;
            }

            String interBrokerURL = "rmi://" + brokerInfo.getIpAddress() + ":" + brokerInfo.getInterBrokerPort() + "/brokerInter_" + brokerInfo.getBrokerID();
            System.out.println("[DEBUG] Attempting to connect to broker at URL: " + interBrokerURL);

            try {
                BrokerInter remoteBroker = (BrokerInter) Naming.lookup(interBrokerURL);
                System.out.println("[DEBUG] Successfully looked up broker: " + interBrokerURL);

                otherBrokers.put(brokerInfo.getBrokerID(), remoteBroker);
                System.out.println("[INFO] Successfully connected to broker: '" + brokerInfo.getBrokerID() + "' at " + interBrokerURL);
            } catch (Exception e) {
                System.err.println("[ERROR] Failed to connect to broker '" + brokerInfo.getBrokerID() + "' at " + interBrokerURL + ".");
                e.printStackTrace();
            }

        } finally {
            System.out.println("[DEBUG] Releasing lock for broker addition.");
            brokerAdditionLock.unlock();
        }
    }


    /**
     * Adds another broker to the network.
     *
     * @param brokerInfo Information about the broker to add.
     * @throws RemoteException If an RMI error occurs.
     */
    @Override
    public void addOtherBroker(BrokerInfo brokerInfo) throws RemoteException {
        addOtherBrokerSafely(brokerInfo);
    }

    /**
     * Retrieves the broker's unique ID.
     *
     * @return Unique identifier of the broker.
     * @throws RemoteException If an RMI error occurs.
     */
    @Override
    public String getBrokerID() throws RemoteException {
        return this.brokerID;
    }

    // ===================== Heartbeat Checker =====================

    /**
     * Starts a separate thread to check for publisher heartbeats and handle failures.
     */
    private void startHeartbeatChecker() {
        new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(5000); // Check every 5 seconds
                    long currentTime = System.currentTimeMillis();
                    List<String> deadPublishers = new ArrayList<>();

                    for (Map.Entry<String, Long> entry : publisherHeartbeats.entrySet()) {
                        if (currentTime - entry.getValue() > 15000) { // No heartbeat for 15 seconds
                            deadPublishers.add(entry.getKey());
                        }
                    }

                    for (String publisherName : deadPublishers) {
                        publisherHeartbeats.remove(publisherName);
                        activePublishers.remove(publisherName);
                        System.out.println("[INFO] Publisher '" + publisherName + "' has crashed or stopped sending heartbeats. Cleaning up.");
                        // Delete all topics from this publisher
                        List<String> topicsToDelete = new ArrayList<>();
                        for (Topic topic : topics.values()) {
                            if (topic.publisherName.equals(publisherName)) {
                                topicsToDelete.add(topic.topicId);
                            }
                        }
                        for (String topicId : topicsToDelete) {
                            try {
                                deleteTopic(topicId);
                            } catch (RemoteException e) {
                                System.err.println("[ERROR] Failed to delete topic ID " + topicId + " due to publisher failure.");
                                e.printStackTrace();
                            }
                        }
                        System.out.println("[INFO] Cleaned up topics for publisher '" + publisherName + "'.");
                    }
                } catch (InterruptedException e) {
                    System.out.println("[ERROR] Heartbeat checker thread interrupted.");
                    e.printStackTrace();
                    break; // Exit the loop if the thread is interrupted
                } catch (Exception e) {
                    System.out.println("[ERROR] Exception in heartbeat checker thread.");
                    e.printStackTrace();
                }
            }
        }, "HeartbeatChecker").start();
    }

    // ===================== Periodic Broker Discovery =====================

    /**
     * Starts a separate thread to periodically discover and connect to new brokers.
     */
    private void startPeriodicBrokerDiscovery() {
        new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(10000); // Discover every 10 seconds
                    List<BrokerInfo> brokerList = directoryService.getActiveBrokers();
                    
                    // Log the current list of active brokers known by the Directory Service
                    System.out.println("[DEBUG] Active brokers in Directory Service: " + brokerList);

                    for (BrokerInfo brokerInfo : brokerList) {
                        if (!brokerInfo.getBrokerID().equals(this.brokerID) && !otherBrokers.containsKey(brokerInfo.getBrokerID())) {
                            System.out.println("[DEBUG] Found new broker '" + brokerInfo.getBrokerID() + "'. Attempting to connect...");
                            addOtherBrokerSafely(brokerInfo);
                        }
                    }
                    
                    // Log the brokers that are currently connected
                    System.out.println("[DEBUG] Currently connected brokers: " + otherBrokers.keySet());

                } catch (InterruptedException e) {
                    System.err.println("[ERROR] Periodic broker discovery thread interrupted.");
                    e.printStackTrace();
                    break;
                } catch (Exception e) {
                    System.err.println("[ERROR] Exception in periodic broker discovery thread.");
                    e.printStackTrace();
                }
            }
        }, "BrokerDiscoveryThread").start();
    }


    // ===================== Helper Methods =====================

    /**
     * Retrieves the topic name given its ID.
     *
     * @param topicId ID of the topic.
     * @return Name of the topic or "Unknown" if not found.
     */
    private String getTopicName(String topicId) {
        Topic topic = topics.get(topicId);
        return (topic != null) ? topic.topicName : "Unknown";
    }

    /**
     * Registers a remote topic locally.
     *
     * @param topicId              ID of the topic.
     * @param topicName            Name of the topic.
     * @param publisherName        Name of the publisher.
     * @param originatingBrokerId  ID of the originating broker.
     * @throws RemoteException If an RMI error occurs.
     */
  
    

    // ===================== Main Method =====================

    /**
     * Main method to start the Broker.
     *
     * @param args Command-line arguments: <ipAddress> <clientCommunicationPort> <interBrokerPort>
     */
    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println("Usage: java Broker <ipAddress> <clientCommunicationPort> <interBrokerPort>");
            System.exit(1);
        }

        String ipAddress = args[0];
        int clientPort;
        int interBrokerPort;
        try {
            clientPort = Integer.parseInt(args[1]);
            interBrokerPort = Integer.parseInt(args[2]);
        } catch (NumberFormatException e) {
            System.out.println("Invalid port number(s). Ports must be integers.");
            return;
        }

        try {
            // Create and start the Broker instance
            Broker broker = new Broker(ipAddress, clientPort, interBrokerPort);
            System.out.println("[SUCCESS] Broker '" + broker.brokerID + "' is running on client port " + clientPort + " and inter-broker port " + interBrokerPort + ".");
        } catch (Exception e) {
            System.out.println("[ERROR] Failed to start Broker.");
            e.printStackTrace();
        }
    }
}
