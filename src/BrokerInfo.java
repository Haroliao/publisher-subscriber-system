//// File: BrokerInfo.java
//import java.io.Serializable;
//
///**
// * BrokerInfo class represents information about a broker.
// * Brokers are uniquely identified by brokerID.
// */
//public class BrokerInfo implements Serializable {
//    private static final long serialVersionUID = 1L;
//    private String brokerID;
//    private String ipAddress;
//    private int port;
//
//    public BrokerInfo(String brokerID, String ipAddress, int port) {
//        this.brokerID = brokerID;
//        this.ipAddress = ipAddress;
//        this.port = port;
//    }
//
//    public String getBrokerID() {
//        return brokerID;
//    }
//
//    public String getIpAddress() {
//        return ipAddress;
//    }
//
//    public int getPort() {
//        return port;
//    }
//
//    @Override
//    public String toString() {
//        return "BrokerInfo{" +
//                "brokerID='" + brokerID + '\'' +
//                ", ipAddress='" + ipAddress + '\'' +
//                ", port=" + port +
//                '}';
//    }
//}

// File: BrokerInfo.java
import java.io.Serializable;

/**
 * BrokerInfo class holds information about a broker.
 */
public class BrokerInfo implements Serializable {
    private static final long serialVersionUID = 1L;
    private String brokerID;
    private String ipAddress;
    private int clientPort;      // Port for client communication
    private int interBrokerPort; // Port for inter-broker communication

    public BrokerInfo(String brokerID, String ipAddress, int clientPort, int interBrokerPort) {
        this.brokerID = brokerID;
        this.ipAddress = ipAddress;
        this.clientPort = clientPort;
        this.interBrokerPort = interBrokerPort;
    }

    public String getBrokerID() {
        return brokerID;
    }

    public String getIpAddress() {
        return ipAddress;
    }

    public int getClientPort() {
        return clientPort;
    }

    public int getInterBrokerPort() {
        return interBrokerPort;
    }

    @Override
    public String toString() {
        return "BrokerInfo{" +
                "brokerID='" + brokerID + '\'' +
                ", ipAddress='" + ipAddress + '\'' +
                ", clientPort=" + clientPort +
                ", interBrokerPort=" + interBrokerPort +
                '}';
    }
}


