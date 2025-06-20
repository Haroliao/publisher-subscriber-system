# publisher-subscriber-system
First start the directory service with java -jar "\path\to\DirectoryService.jar"

Then initialize the broker with java -jar "\path\to\Broker.jar" xxx1 xxx2 
xxx1 is the self defined number for the client communication port that is used by clients (publishers/subscribers) to communicate with the broke, xx2 is the self defined number for the inter-broker communication port tahat is used for communication between brokers.

Create publisher with java -jar "\path\to\Publisher.jar" xx
xx is the self defined publisher name for each publisher

Create subscriber with java -jar "\path\to\Subscriber.jar" xx
xx is the self defined subscriber name for each subscriber
