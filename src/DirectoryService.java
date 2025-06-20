// File: DirectoryService.java
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * DirectoryService class implements the DirectoryServiceInter interface and manages broker registrations
 * and topic assignments.
 */
public class DirectoryService extends UnicastRemoteObject implements DirectoryServiceInter {

    private static final long serialVersionUID = 1L;

    // Map of brokerID to BrokerInfo
    private final Map<String, BrokerInfo> activeBrokers;

    // Map of topicId to brokerID
    private final Map<String, String> topicToBrokerMap;

    // For load balancing (e.g., round-robin)
    private final List<String> brokerList;
    private int currentBrokerIndex;

    /**
     * Constructor for DirectoryService.
     *
     * @throws RemoteException If an RMI error occurs.
     */
    public DirectoryService() throws RemoteException {
        super();
        activeBrokers = new ConcurrentHashMap<>();
        topicToBrokerMap = new ConcurrentHashMap<>();
        brokerList = Collections.synchronizedList(new ArrayList<>());
        currentBrokerIndex = 0;
    }

    /**
     * Registers a new broker with the Directory Service.
     *
     * @param brokerID  Unique identifier for the broker.
     * @param ipAddress IP address of the broker.
     * @param port      Communication port of the broker.
     * @throws RemoteException If an RMI error occurs.
     */
    @Override
    public synchronized void registerBroker(String brokerID, String ipAddress, int clientPort, int interBrokerPort) throws RemoteException {
        if (!activeBrokers.containsKey(brokerID)) {
            // Create BrokerInfo with all four parameters
            BrokerInfo brokerInfo = new BrokerInfo(brokerID, ipAddress, clientPort, interBrokerPort);
            activeBrokers.put(brokerID, brokerInfo);
            brokerList.add(brokerID);
            System.out.println("[DirectoryService] Registered broker: " + brokerInfo);
        } else {
            System.out.println("[DirectoryService] Broker " + brokerID + " is already registered.");
        }
    }


    /**
     * Deregisters a broker from the Directory Service.
     *
     * @param brokerID Unique identifier for the broker.
     * @throws RemoteException If an RMI error occurs.
     */
    @Override
    public synchronized void deregisterBroker(String brokerID) throws RemoteException {
        BrokerInfo removed = activeBrokers.remove(brokerID);
        if (removed != null) {
            brokerList.remove(brokerID);
            // Remove all topics assigned to this broker
            List<String> topicsToReassign = new ArrayList<>();
            for (Map.Entry<String, String> entry : topicToBrokerMap.entrySet()) {
                if (entry.getValue().equals(brokerID)) {
                    topicsToReassign.add(entry.getKey());
                }
            }
            for (String topicId : topicsToReassign) {
                assignTopicToBroker(topicId, getNextBrokerID());
            }
            System.out.println("[DirectoryService] Deregistered broker: " + brokerID);
        } else {
            System.out.println("[DirectoryService] Broker " + brokerID + " is not registered.");
        }
    }

    /**
     * Assigns a topic to a broker. Implements a simple round-robin assignment.
     *
     * @param topicId  Unique identifier for the topic.
     * @param brokerID Unique identifier for the broker.
     * @throws RemoteException If an RMI error occurs or brokerID is invalid.
     */
    @Override
    public synchronized void assignTopicToBroker(String topicId, String brokerID) throws RemoteException {
        if (brokerID == null) {
            throw new RemoteException("brokerID cannot be null for topic assignment.");
        }
        if (!activeBrokers.containsKey(brokerID)) {
            throw new RemoteException("Broker ID " + brokerID + " is not registered.");
        }
        topicToBrokerMap.put(topicId, brokerID);
        System.out.println("[DirectoryService] Assigned topic ID " + topicId + " to broker " + brokerID + ".");
    }

    /**
     * Retrieves the broker responsible for a given topic.
     *
     * @param topicId Unique identifier for the topic.
     * @return brokerID responsible for the topic, or null if not assigned.
     * @throws RemoteException If an RMI error occurs.
     */
    @Override
    public synchronized String getBrokerForTopic(String topicId) throws RemoteException {
        return topicToBrokerMap.get(topicId);
    }

    /**
     * Provides a broker for a client, implementing a simple round-robin selection for load balancing.
     *
     * @return BrokerInfo object of the selected broker.
     * @throws RemoteException If no brokers are registered.
     */
    @Override
    public synchronized BrokerInfo getBrokerForClient() throws RemoteException {
        if (brokerList.isEmpty()) {
            throw new RemoteException("No active brokers available.");
        }
        String brokerID = getNextBrokerID();
        return activeBrokers.get(brokerID);
    }

    /**
     * Retrieves all active brokers.
     *
     * @return List of BrokerInfo objects.
     * @throws RemoteException If an RMI error occurs.
     */
    @Override
    public synchronized List<BrokerInfo> getActiveBrokers() throws RemoteException {
        return new ArrayList<>(activeBrokers.values());
    }

    /**
     * Gets the next broker ID in a round-robin fashion.
     *
     * @return brokerID string.
     */
    private synchronized String getNextBrokerID() {
        if (brokerList.isEmpty()) {
            return null;
        }
        String brokerID = brokerList.get(currentBrokerIndex);
        currentBrokerIndex = (currentBrokerIndex + 1) % brokerList.size();
        return brokerID;
    }

    // Additional methods as needed...
}

