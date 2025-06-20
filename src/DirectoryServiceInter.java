// File: DirectoryServiceInter.java
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

/**
 * DirectoryServiceInter interface defines the remote methods for directory service operations.
 */
public interface DirectoryServiceInter extends Remote {

    // Update this method to include the new parameter for inter-broker communication port
    void registerBroker(String brokerID, String ipAddress, int clientPort, int interBrokerPort) throws RemoteException;

    void deregisterBroker(String brokerID) throws RemoteException;

    List<BrokerInfo> getActiveBrokers() throws RemoteException;

    void assignTopicToBroker(String topicId, String brokerID) throws RemoteException;

    String getBrokerForTopic(String topicId) throws RemoteException;

    BrokerInfo getBrokerForClient() throws RemoteException;
}
