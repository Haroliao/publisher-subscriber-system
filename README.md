# publisher-subscriber-system
The pub-sub system uses a centralized broker model, where brokers act as intermediaries between a network of publishers and subscribers. Publishers can create and delete topics, publish topic specfic messages and see each topic’s subscriber counts. Subscribers can view, subscribe and unsubscribe from all publishers' created topics, and they can also view a list of their current subscriptions and receive messages published to the topics they are subscribed to

First start the directory service with java -jar "\path\to\DirectoryService.jar"

Create any broker with the command java -jar "\path\to\Broker.jar" xxx1 xxx2 
xxx1 is the self defined number for the client communication port that is used by clients (publishers/subscribers) to communicate with the broke, xx2 is the self defined number for the inter-broker communication port that is used for communication between brokers. xxx1 and xx2 has to be different for each new broker

Create any publisher with the command java -jar "\path\to\Publisher.jar" xx
xx is the self defined publisher name for each publisher, name of different publisher can be the same

Create any subscriber with java -jar "\path\to\Subscriber.jar" xx
xx is the self defined subscriber name for each subscriber, name of different subscriber can be the same
