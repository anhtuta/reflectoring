package hello.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Component;
import hello.model.User;

/**
 * A KafkaMessageListenerContainer receives all messages from all topics on a single thread.
 * 
 * A ConcurrentMessageListenerContainer assigns these messages to multiple
 * KafkaMessageListenerContainer instances to provide multi-threaded capability
 */
@Component
public class KafkaListenersExample {

    Logger LOG = LoggerFactory.getLogger(KafkaListenersExample.class);

    @KafkaListener(topics = "reflectoring-1")
    public void listener(String message) {
        LOG.info("Listener [{}]", message);
    }

    @KafkaListener(
            topics = "reflectoring-1, reflectoring-2",
            groupId = "reflectoring-group-2")
    public void commonListenerForMultipleTopics(String message) {
        LOG.info("MultipleTopicListener - [{}]", message);
    }

    // configure listeners to listen to multiple topics, partitions, and a specific initial offset
    @KafkaListener(
            topicPartitions = @TopicPartition(
                    topic = "reflectoring-3",
                    partitionOffsets = {@PartitionOffset(partition = "0", initialOffset = "0")}),
            groupId = "reflectoring-group-3")
    void listenToParitionWithOffset(@Payload String message, @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
            @Header(KafkaHeaders.OFFSET) int offset) {
        LOG.info("ListenToPartitionWithOffset [{}] from partition-{} with offset-{}", message, partition, offset);
    }

    // Spring allows sending method’s return value to the specified destination with @SendTo:
    // sending the reply message to the topic “reflectoring-2”.
    @KafkaListener(topics = "reflectoring-others")
    @SendTo("reflectoring-2")
    String listenAndReply(String message) {
        LOG.info("ListenAndReply [{}]", message);
        return "This is a reply sent to 'reflectoring-2' topic after receiving message at 'reflectoring-others' topic";
    }

    /**
     * listen to User objects
     * 
     * Since we have multiple listener containers, we are specifying which container factory to use.
     * 
     * If we don’t specify the containerFactory attribute it defaults to kafkaListenerContainerFactory
     * which uses StringSerializer and StringDeserializer in our case
     */
    @KafkaListener(
            id = "1",
            topics = "reflectoring-user",
            groupId = "reflectoring-group-user",
            containerFactory = "userKafkaListenerContainerFactory")
    public void listener(User user) {
        LOG.info("CustomUserListener [{}]", user);
    }

    @KafkaListener(id = "2", topics = "reflectoring-user", groupId = "reflectoring-user-mc",
            containerFactory = "kafkaJsonListenerContainerFactory")
    void listenerWithMessageConverter(User user) {
        LOG.info("MessageConverterUserListener [{}]", user);
    }

    @KafkaListener(topics = "reflectoring-bytes")
    void listenerForRoutingTemplate(String message) {
        LOG.info("RoutingTemplate BytesListener [{}]", message);
    }
}
