// DirectoryServiceServer.java

import java.rmi.Naming;
import java.rmi.registry.LocateRegistry;

public class DirectoryServiceServer {
    public static void main(String[] args) {
        try {
            // Start the RMI registry programmatically on port 1099
            LocateRegistry.createRegistry(1099);
            System.out.println("Java RMI registry created on port 1099.");

            // Create an instance of DirectoryService
            DirectoryServiceInter directoryService = new DirectoryService();

            // Bind the DirectoryService instance to the RMI registry
            Naming.rebind("rmi://localhost:1099/DirectoryService", directoryService);
            System.out.println("Directory Service is running and bound to 'DirectoryService'.");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
