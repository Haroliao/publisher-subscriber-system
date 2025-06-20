// File: TopicInfo.java
import java.io.Serializable;

/**
 * TopicInfo represents the information of a Topic.
 */
public class TopicInfo implements Serializable {
    private static final long serialVersionUID = 1L;
    
    private String topicId;
    private String topicName;
    private String publisherName;

    public TopicInfo(String topicId, String topicName, String publisherName) {
        this.topicId = topicId;
        this.topicName = topicName;
        this.publisherName = publisherName;
    }

    // Getters
    public String getTopicId() {
        return topicId;
    }

    public String getTopicName() {
        return topicName;
    }

    public String getPublisherName() {
        return publisherName;
    }

    // Optionally, override toString() for better readability
    @Override
    public String toString() {
        return "TopicInfo{" +
                "topicId='" + topicId + '\'' +
                ", topicName='" + topicName + '\'' +
                ", publisherName='" + publisherName + '\'' +
                '}';
    }
}
