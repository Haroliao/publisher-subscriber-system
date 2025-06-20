
import java.rmi.Remote;
import java.rmi.RemoteException;

public interface SubscriberCallbacks extends Remote {
    // Method to receive a published message
    void receiveMessage(String topicId, String topicName, String message, String publisherName) throws RemoteException;

    // Method to receive notification when a topic is deleted
    void notifyTopicDeletion(String topicId, String topicName) throws RemoteException;
}
