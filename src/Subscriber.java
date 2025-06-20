
// File: Subscriber.java
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Subscriber class implements the SubscriberInterface and provides a console-based
 * interface for users to interact with the Publish-Subscribe system.
 */
public class Subscriber extends UnicastRemoteObject implements SubscriberInterface {

    private static final long serialVersionUID = 1L;

    // Subscriber name (unique identifier)
    private String subscriberName;

    // Current subscriptions: topicId -> TopicInfo
    private Map<String, TopicInfo> currentSubscriptions;

    // Reference to the Directory Service
    private DirectoryServiceInter directoryService;

    // Reference to the Broker
    private BrokerInter broker;

    // Scanner for console input
    private transient Scanner scanner;

    // Host and port of the Directory Service RMI Registry
    private static final String DIRECTORY_SERVICE_HOST = "localhost";
    private static final int DIRECTORY_SERVICE_PORT = 1099;

    private boolean isCleanedUp = false; // Flag to prevent multiple cleanups

    /**
     * Constructor for Subscriber.
     *
     * @param subscriberName Name of the subscriber.
     * @throws RemoteException If a remote error occurs.
     */
    protected Subscriber(String subscriberName) throws RemoteException {
        super();
        this.subscriberName = subscriberName;
        this.currentSubscriptions = new ConcurrentHashMap<>();
        this.scanner = new Scanner(System.in);
        connectToDirectoryService();
        connectToBroker();

        // Register Shutdown Hook for graceful termination
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\n[INFO] Shutdown Hook triggered. Performing cleanup...");
            cleanup();
        }));
    }

    /**
     * Connects to the Directory Service to obtain a reference.
     */
    private void connectToDirectoryService() {
        try {
            String directoryServiceURL = "rmi://" + DIRECTORY_SERVICE_HOST + ":" + DIRECTORY_SERVICE_PORT + "/DirectoryService";
            directoryService = (DirectoryServiceInter) Naming.lookup(directoryServiceURL);
            System.out.println("[INFO] Connected to Directory Service at " + directoryServiceURL + ".");
        } catch (Exception e) {
            System.err.println("[ERROR] Failed to connect to Directory Service.");
            e.printStackTrace();
            cleanup(); // Automatically cleanup on exception
            System.exit(1);
        }
    }

    /**
     * Connects to an active Broker using round-robin selection from the Directory Service.
     */
    private void connectToBroker() {
        try {
            BrokerInfo brokerInfo = directoryService.getBrokerForClient();
            String brokerURL = "rmi://" + brokerInfo.getIpAddress() + ":" + brokerInfo.getClientPort() + "/brokerClient_" + brokerInfo.getBrokerID();
            broker = (BrokerInter) Naming.lookup(brokerURL);
            System.out.println("[INFO] Connected to Broker '" + brokerInfo.getBrokerID() + "' at " + brokerURL + ".");

            // Register this subscriber with the Broker
            try {
                broker.registerSubscriberCallback(subscriberName, this);
                System.out.println("[INFO] Registered subscriber '" + subscriberName + "' with Broker.");
            } catch (RemoteException re) {
                System.err.println("[ERROR] RemoteException during subscriber registration.");
                re.printStackTrace();
                cleanup(); // Automatically cleanup on exception
                System.exit(1);
            } catch (Exception e) {
                System.err.println("[ERROR] Exception during subscriber registration.");
                e.printStackTrace();
                cleanup(); // Automatically cleanup on exception
                System.exit(1);
            }

        } catch (Exception e) {
            System.err.println("[ERROR] Failed to connect to Broker.");
            e.printStackTrace();
            cleanup(); // Automatically cleanup on exception
            System.exit(1);
        }
    }

    /**
     * Receives a message from a subscribed topic.
     *
     * @param topicId   ID of the topic.
     * @param topicName Name of the topic.
     * @param message   The message content.
     * @throws RemoteException If a remote communication error occurs.
     */
    @Override
    public void receiveMessage(String topicId, String topicName, String message, String publisherName) throws RemoteException {
        System.out.println("\n[MESSAGE RECEIVED] Topic ID: " + topicId + ", Topic Name: " + topicName + ", Publisher: " + publisherName + ", Message: " + message);
        System.out.print("Please select command: ");
    }

    /**
     * Notifies the subscriber about the deletion of a subscribed topic.
     *
     * @param topicId   ID of the deleted topic.
     * @param topicName Name of the deleted topic.
     * @throws RemoteException If a remote communication error occurs.
     */
    @Override
    public void notifyTopicDeletion(String topicId, String topicName) throws RemoteException {
        System.out.println("\n[NOTIFICATION] Topic deleted: " + topicName + " (" + topicId + ")");
        currentSubscriptions.remove(topicId);
        System.out.print("Please select command: ");
    }

    /**
     * Performs cleanup by unsubscribing from all current subscriptions.
     */
    public synchronized void cleanup() {
        if (isCleanedUp) {
            return;
        }
        isCleanedUp = true;
        System.out.println("[INFO] Performing cleanup: unsubscribing from all topics.");

        // Create a copy of the topic IDs to avoid ConcurrentModificationException
        Map<String, TopicInfo> subscriptionsToRemove = new HashMap<>(this.currentSubscriptions);
        for (String topicId : subscriptionsToRemove.keySet()) {
            try {
                broker.unsubscribe(topicId, subscriberName);
                System.out.println("[INFO] Unsubscribed from topic ID: " + topicId);
            } catch (RemoteException e) {
                System.err.println("[ERROR] Failed to unsubscribe from topic ID: " + topicId + " during cleanup.");
                e.printStackTrace();
            }
        }
    }

    /**
     * Starts the console interface for user commands.
     */
    public void startConsole() {
        System.out.println("Welcome, Subscriber '" + subscriberName + "'!");
        System.out.println("Available commands: list, sub, current, unsub, exit");
        while (true) {
            try {
                System.out.print("Please select command: ");
                String input = scanner.nextLine().trim();
                if (input.isEmpty()) {
                    continue;
                }

                String[] parts = input.split("\\s+", 2); // Split into at most 2 parts
                String command = parts[0].toLowerCase();

                switch (command) {
                    case "list":
                        handleListCommand();
                        break;
                    case "sub":
                        if (parts.length < 2) {
                            System.out.println("[ERROR] Usage: sub {topic_id}");
                        } else {
                            handleSubscribeCommand(parts[1]);
                        }
                        break;
                    case "current":
                        handleCurrentCommand();
                        break;
                    case "unsub":
                        if (parts.length < 2) {
                            System.out.println("[ERROR] Usage: unsub {topic_id}");
                        } else {
                            handleUnsubscribeCommand(parts[1]);
                        }
                        break;
                    case "exit":
                        handleExitCommand();
                        return;
                    default:
                        System.out.println("[ERROR] Unknown command. Available commands: list, sub, current, unsub, exit");
                }
            } catch (Exception e) {
                System.err.println("[ERROR] An exception occurred while processing commands.");
                e.printStackTrace();
                cleanup(); // Automatically cleanup on exception
                System.exit(1); // Exit after cleanup
            }
        }
    }

    /**
     * Handles the 'list' command to list all available topics.
     */
    private void handleListCommand() {
        try {
            List<Map<String, String>> topics = broker.listAllTopics(null);
            if (topics.isEmpty()) {
                System.out.println("[INFO] No topics available.");
                return;
            }

            // Use a Set to keep track of unique topic IDs
            Set<String> uniqueTopicIds = new HashSet<>();
            System.out.println("Available Topics:");
            System.out.printf("%-15s %-30s %-20s%n", "Topic ID", "Topic Name", "Publisher");

            for (Map<String, String> topic : topics) {
                String topicId = topic.get("topicId");

                // Skip the topic if the ID is already in the set
                if (uniqueTopicIds.contains(topicId)) {
                    continue;
                }

                uniqueTopicIds.add(topicId);
                System.out.printf("%-15s %-30s %-20s%n",
                        topicId,
                        topic.get("topicName"),
                        topic.get("publisherName"));
            }
        } catch (RemoteException e) {
            System.err.println("[ERROR] Failed to retrieve topics from Broker.");
            e.printStackTrace();
            cleanup(); // Automatically cleanup on exception
        } catch (Exception e) {
            System.err.println("[ERROR] Exception while listing topics.");
            e.printStackTrace();
            cleanup(); // Automatically cleanup on exception
        }
    }

    /**
     * Handles the 'sub' command to subscribe to a topic.
     *
     * @param topicId ID of the topic to subscribe to.
     */
    private void handleSubscribeCommand(String topicId) {
        try {
            // Check if already subscribed
            if (currentSubscriptions.containsKey(topicId)) {
                System.out.println("[WARN] Already subscribed to topic ID " + topicId + ".");
                return;
            }

            // Subscribe via Broker
            broker.subscribe(topicId, subscriberName, this);
            // Retrieve topic details
            List<Map<String, String>> topics = broker.listAllTopics(null);
            String topicName = "Unknown";
            String publisherName = "Unknown";
            for (Map<String, String> topic : topics) {
                if (topic.get("topicId").equals(topicId)) {
                    topicName = topic.get("topicName");
                    publisherName = topic.get("publisherName");
                    break;
                }
            }
            currentSubscriptions.put(topicId, new TopicInfo(topicId, topicName, publisherName));
            System.out.println("[SUCCESS] Subscribed to topic '" + topicName + "' (" + topicId + ").");
        } catch (RemoteException e) {
            System.err.println("[ERROR] Failed to subscribe to topic ID " + topicId + ".");
            e.printStackTrace();
            cleanup(); // Automatically cleanup on exception
        } catch (Exception e) {
            System.err.println("[ERROR] Exception while subscribing to topic ID " + topicId + ".");
            e.printStackTrace();
            cleanup(); // Automatically cleanup on exception
        }
    }

    /**
     * Handles the 'current' command to display current subscriptions.
     */
    private void handleCurrentCommand() {
        if (currentSubscriptions.isEmpty()) {
            System.out.println("[INFO] No current subscriptions.");
            return;
        }
        System.out.println("Current Subscriptions:");
        System.out.printf("%-15s %-30s %-20s%n", "Topic ID", "Topic Name", "Publisher");
        for (TopicInfo topic : currentSubscriptions.values()) {
            System.out.printf("%-15s %-30s %-20s%n",
                    topic.getTopicId(),
                    topic.getTopicName(),
                    topic.getPublisherName());
        }
    }

    /**
     * Handles the 'unsub' command to unsubscribe from a topic.
     *
     * @param topicId ID of the topic to unsubscribe from.
     */
    private void handleUnsubscribeCommand(String topicId) {
        try {
            if (!currentSubscriptions.containsKey(topicId)) {
                System.out.println("[WARN] Not subscribed to topic ID " + topicId + ".");
                return;
            }
            broker.unsubscribe(topicId, subscriberName);
            TopicInfo removed = currentSubscriptions.remove(topicId);
            System.out.println("[SUCCESS] Unsubscribed from topic '" + removed.getTopicName() + "' (" + topicId + ").");
        } catch (RemoteException e) {
            System.err.println("[ERROR] Failed to unsubscribe from topic ID " + topicId + ".");
            e.printStackTrace();
            cleanup(); // Automatically cleanup on exception
        } catch (Exception e) {
            System.err.println("[ERROR] Exception while unsubscribing from topic ID " + topicId + ".");
            e.printStackTrace();
            cleanup(); // Automatically cleanup on exception
        }
    }

    /**
     * Handles the 'exit' command to gracefully exit the Subscriber.
     */
    private void handleExitCommand() {
        try {
            // Unregister from Broker
            broker.unregisterSubscriberCallback(subscriberName);
            System.out.println("[INFO] Unregistered from Broker.");
            cleanup(); // Ensure all subscriptions are removed
            System.out.println("Exiting Subscriber. Goodbye!");
        } catch (RemoteException e) {
            System.err.println("[ERROR] Failed to unregister from Broker.");
            e.printStackTrace();
            cleanup(); // Ensure cleanup even if unregister fails
        } finally {
            try {
                UnicastRemoteObject.unexportObject(this, true);
            } catch (Exception e) {
                // Ignore errors during unexport
            }
            System.exit(0);
        }
    }

    /**
     * Main method to start the Subscriber.
     *
     * @param args Command-line arguments: <subscriberName>
     */
    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Usage: java Subscriber <subscriberName>");
            System.exit(1);
        }

        String subscriberName = args[0];
        try {
            Subscriber subscriber = new Subscriber(subscriberName);
            subscriber.startConsole();
        } catch (RemoteException e) {
            System.err.println("[ERROR] Failed to start Subscriber.");
            e.printStackTrace();
        }
    }

    /**
     * Inner class to represent Topic Information.
     */
    private static class TopicInfo {
        private String topicId;
        private String topicName;
        private String publisherName;

        public TopicInfo(String topicId, String topicName, String publisherName) {
            this.topicId = topicId;
            this.topicName = topicName;
            this.publisherName = publisherName;
        }

        public String getTopicId() {
            return topicId;
        }

        public String getTopicName() {
            return topicName;
        }

        public String getPublisherName() {
            return publisherName;
        }
    }
}
