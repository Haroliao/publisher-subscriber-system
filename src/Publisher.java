
// File: Publisher.java
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

/**
 * Represents a Publisher in the distributed system.
 * A Publisher can create topics, publish messages to topics, show subscriber counts, and delete topics via the Broker.
 */
public class Publisher {

    private final String publisherName;
    private BrokerInter broker;
    private Map<String, String> topics; // Maps topicId to topicName
    private boolean isCleanedUp = false; // Flag to prevent multiple cleanups

    /**
     * Constructs a Publisher with the given name and establishes a connection to the Broker via the Directory Service.
     *
     * @param publisherName Name of the publisher.
     */
    public Publisher(String publisherName) {
        this.publisherName = publisherName;
        this.topics = new HashMap<>(); // Initialize the topics map

        try {
            // Directory Service configuration
            String directoryServiceIp = "localhost"; // Directory service IP, change if needed
            int directoryServicePort = 1099;         // Directory service port, change if needed
            System.out.println("[INFO] Connecting to Directory Service at " + directoryServiceIp + ":" + directoryServicePort);

            // Obtain reference to Directory Service Registry
            Registry directoryRegistry = LocateRegistry.getRegistry(directoryServiceIp, directoryServicePort);

            // Lookup the Directory Service from the registry
            DirectoryServiceInter directoryService = (DirectoryServiceInter) directoryRegistry.lookup("DirectoryService");
            System.out.println("[INFO] Connected to Directory Service.");

            // Retrieve a Broker for the publisher using assignment logic (e.g., round-robin)
            BrokerInfo brokerInfo = directoryService.getBrokerForClient();
            System.out.println("[INFO] Retrieved Broker Info: " + brokerInfo);

            // Validate BrokerInfo
            if (brokerInfo == null) {
                throw new RemoteException("No BrokerInfo received from Directory Service.");
            }

            // Obtain the registry from the Broker's client port
            System.out.println("[INFO] Connecting to Broker at " + brokerInfo.getIpAddress() + ":" + brokerInfo.getClientPort());
            Registry brokerRegistry = LocateRegistry.getRegistry(brokerInfo.getIpAddress(), brokerInfo.getClientPort());

            // Perform lookup for the Broker's remote object
            String brokerBindingName = "brokerClient_" + brokerInfo.getBrokerID();
            System.out.println("[DEBUG] Looking up Broker with binding name: " + brokerBindingName);
            broker = (BrokerInter) brokerRegistry.lookup(brokerBindingName);
            System.out.println("[INFO] Connected to Broker: " + brokerBindingName);

            // Register publisher with the broker
            try {
                System.out.println("[DEBUG] Attempting to register Publisher '" + publisherName + "' with Broker: " + brokerBindingName);
                broker.registerPublisher(publisherName);
                System.out.println("[SUCCESS] Publisher '" + publisherName + "' registered with Broker: " + brokerBindingName);
            } catch (RemoteException re) {
                System.err.println("[ERROR] RemoteException during publisher registration.");
                re.printStackTrace();
                cleanup(); // Automatically cleanup on exception
                System.exit(1);
            } catch (Exception e) {
                System.err.println("[ERROR] Exception during publisher registration.");
                e.printStackTrace();
                cleanup(); // Automatically cleanup on exception
                System.exit(1);
            }

            // Register Shutdown Hook for graceful termination
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("\n[INFO] Shutdown Hook triggered. Performing cleanup...");
                cleanup();
            }));

        } catch (RemoteException re) {
            System.err.println("[ERROR] RemoteException occurred while connecting to Directory Service or Broker.");
            re.printStackTrace();
            cleanup(); // Automatically cleanup on exception
            System.exit(1);
        } catch (Exception e) {
            System.err.println("[ERROR] Exception occurred while setting up Publisher.");
            e.printStackTrace();
            cleanup(); // Automatically cleanup on exception
            System.exit(1);
        }
    }

    /**
     * Performs cleanup by deleting all topics created by this Publisher.
     */
    public synchronized void cleanup() {
        if (isCleanedUp) {
            return;
        }
        isCleanedUp = true;
        System.out.println("[INFO] Performing cleanup: deleting all topics.");

        // Create a copy of the topic IDs to avoid ConcurrentModificationException
        Map<String, String> topicsToDelete = new HashMap<>(this.topics);
        for (String topicId : topicsToDelete.keySet()) {
            try {
                broker.deleteTopic(topicId);
                System.out.println("[INFO] Deleted topic '" + topics.get(topicId) + "' with ID: " + topicId);
            } catch (RemoteException e) {
                System.err.println("[ERROR] Failed to delete topic ID: " + topicId + " during cleanup.");
                e.printStackTrace();
            }
        }
    }

    /**
     * Creates a new topic on the connected Broker.
     *
     * @param topicId   Unique identifier for the topic.
     * @param topicName Name of the topic.
     */
    public void createTopic(String topicId, String topicName) {
        if (topics.containsKey(topicId)) {
            System.out.println("[ERROR] Topic ID: " + topicId + " already exists.");
            return;
        }

        try {
            broker.createTopic(topicId, topicName, publisherName);
            topics.put(topicId, topicName); // Add the new topic to the map
            System.out.println("[SUCCESS] Topic created: '" + topicName + "' with ID: " + topicId);
        } catch (RemoteException re) {
            System.err.println("[ERROR] RemoteException while creating topic: '" + topicName + "'.");
            re.printStackTrace();
            cleanup(); // Automatically cleanup on exception
        } catch (Exception e) {
            System.err.println("[ERROR] Exception while creating topic: '" + topicName + "'.");
            e.printStackTrace();
            cleanup(); // Automatically cleanup on exception
        }
    }

    /**
     * Publishes a message to a specific topic on the connected Broker.
     *
     * @param topicId ID of the topic.
     * @param message Message to be published.
     */
    public void publishMessage(String topicId, String message) {
        if (message.length() > 100) {
            System.out.println("[ERROR] Message exceeds 100 characters limit.");
            return;
        }

        if (!topics.containsKey(topicId)) {
            System.out.println("[ERROR] Topic ID: " + topicId + " not found among your created topics.");
            return;
        }

        try {
            broker.publishMessage(topicId, message, publisherName);
            System.out.println("[SUCCESS] Message published to topic ID: " + topicId);
        } catch (RemoteException re) {
            System.err.println("[ERROR] RemoteException while publishing message to topic ID: " + topicId + ".");
            re.printStackTrace();
            cleanup(); // Automatically cleanup on exception
        } catch (Exception e) {
            System.err.println("[ERROR] Exception while publishing message to topic ID: " + topicId + ".");
            e.printStackTrace();
            cleanup(); // Automatically cleanup on exception
        }
    }

    /**
     * Shows the subscriber counts for a specific topic or all topics associated with this publisher.
     *
     * @param topicId Optional topic ID. If null, show counts for all topics.
     */
    public void showSubscriberCounts(String topicId) {
        if (topicId != null) {
            if (!topics.containsKey(topicId)) {
                System.out.println("[ERROR] Topic ID: " + topicId + " not found among your created topics.");
                return;
            }

            System.out.println("\nSubscriber Count for Topic: " + topicId);
            System.out.println("---------------------------------------------------------------");
            System.out.printf("%-36s %-20s %-10s%n", "Topic ID", "Topic Name", "Subscribers");
            System.out.println("---------------------------------------------------------------");
            String topicName = topics.get(topicId);
            try {
                int count = broker.showSubscriberCount(topicId);
                System.out.printf("%-36s %-20s %-10d%n", topicId, topicName, count);
            } catch (RemoteException re) {
                System.err.println("[ERROR] RemoteException while fetching subscriber count for topic ID: " + topicId + ".");
                re.printStackTrace();
                cleanup(); // Automatically cleanup on exception
            } catch (Exception e) {
                System.err.println("[ERROR] Exception while fetching subscriber count for topic ID: " + topicId + ".");
                e.printStackTrace();
                cleanup(); // Automatically cleanup on exception
            }
            System.out.println("---------------------------------------------------------------");
        } else {
            // Show subscriber counts for all topics if no specific topicId is provided
            if (topics.isEmpty()) {
                System.out.println("[INFO] No topics to display.");
                return;
            }

            System.out.println("\nSubscriber Counts for All Topics:");
            System.out.println("---------------------------------------------------------------");
            System.out.printf("%-36s %-20s %-10s%n", "Topic ID", "Topic Name", "Subscribers");
            System.out.println("---------------------------------------------------------------");
            for (Map.Entry<String, String> entry : topics.entrySet()) {
                String id = entry.getKey();
                String name = entry.getValue();
                try {
                    int count = broker.showSubscriberCount(id);
                    System.out.printf("%-36s %-20s %-10d%n", id, name, count);
                } catch (RemoteException re) {
                    System.err.println("[ERROR] RemoteException while fetching subscriber count for topic ID: " + id + ".");
                    re.printStackTrace();
                    cleanup(); // Automatically cleanup on exception
                } catch (Exception e) {
                    System.err.println("[ERROR] Exception while fetching subscriber count for topic ID: " + id + ".");
                    e.printStackTrace();
                    cleanup(); // Automatically cleanup on exception
                }
            }
            System.out.println("---------------------------------------------------------------");
        }
    }

    /**
     * Deletes a topic from the connected Broker.
     *
     * @param topicId ID of the topic to delete.
     */
    public void deleteTopic(String topicId) {
        if (!topics.containsKey(topicId)) {
            System.out.println("[ERROR] Topic ID: " + topicId + " not found among your created topics.");
            return;
        }

        try {
            broker.deleteTopic(topicId);
            String topicName = topics.remove(topicId); // Remove the topic from the map
            System.out.println("[SUCCESS] Topic deleted: '" + topicName + "' with ID: " + topicId);
        } catch (RemoteException re) {
            System.err.println("[ERROR] RemoteException while deleting topic with ID: " + topicId + ".");
            re.printStackTrace();
            cleanup(); // Automatically cleanup on exception
        } catch (Exception e) {
            System.err.println("[ERROR] Exception while deleting topic with ID: " + topicId + ".");
            e.printStackTrace();
            cleanup(); // Automatically cleanup on exception
        }
    }

    /**
     * Displays the available commands to the user.
     */
    public void displayHelp() {
        System.out.println("\nAvailable commands:");
        System.out.println("1. create {topic_id} {topic_name}       # Create a new topic");
        System.out.println("2. publish {topic_id} {message}         # Publish a message to an existing topic");
        System.out.println("3. show [topic_id]                      # Show subscriber counts for a specific or all topics");
        System.out.println("4. delete {topic_id}                    # Delete a topic");
        System.out.println("5. help                                 # Display this help message");
    }

    /**
     * Main method to start the Publisher.
     *
     * @param args Command-line arguments: <publisherName>
     */
    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Usage: java Publisher <publisherName>");
            System.exit(1);
        }

        String publisherName = args[0];
        Publisher publisher = new Publisher(publisherName);

        Scanner scanner = new Scanner(System.in);
        System.out.println("\nWelcome, Publisher '" + publisherName + "'!");
        publisher.displayHelp();

        while (true) {
            System.out.print("\nPlease select command (create, publish, show, delete, help): ");
            String input = scanner.nextLine().trim();

            if (input.isEmpty()) {
                continue; // Skip empty inputs
            }

            String[] tokens = input.split(" ", 3); // Split into at most 3 parts
            String command = tokens[0].toLowerCase();

            switch (command) {
                case "create":
                    if (tokens.length < 3) {
                        System.out.println("[ERROR] Invalid syntax. Usage: create {topic_id} {topic_name}");
                    } else {
                        String topicId = tokens[1];
                        String topicName = tokens[2];
                        publisher.createTopic(topicId, topicName);
                    }
                    break;

                case "publish":
                    if (tokens.length < 3) {
                        System.out.println("[ERROR] Invalid syntax. Usage: publish {topic_id} {message}");
                    } else {
                        String topicId = tokens[1];
                        String message = tokens[2];
                        publisher.publishMessage(topicId, message);
                    }
                    break;

                case "show":
                    if (tokens.length > 2) {
                        System.out.println("[ERROR] Invalid syntax. Usage: show [topic_id]");
                    } else if (tokens.length == 2) {
                        String topicId = tokens[1];
                        publisher.showSubscriberCounts(topicId);
                    } else {
                        publisher.showSubscriberCounts(null); // Show all topics
                    }
                    break;

                case "delete":
                    if (tokens.length < 2) {
                        System.out.println("[ERROR] Invalid syntax. Usage: delete {topic_id}");
                    } else {
                        String topicId = tokens[1];
                        publisher.deleteTopic(topicId);
                    }
                    break;

                case "help":
                    publisher.displayHelp();
                    break;

                default:
                    System.out.println("[ERROR] Unknown command. Type 'help' to see available commands.");
                    break;
            }
        }
    }
}
