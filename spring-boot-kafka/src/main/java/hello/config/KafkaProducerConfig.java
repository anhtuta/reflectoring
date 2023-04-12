package hello.config;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.core.RoutingKafkaTemplate;
import org.springframework.kafka.support.ProducerListener;
import org.springframework.kafka.support.converter.StringJsonMessageConverter;
import org.springframework.kafka.support.serializer.JsonSerializer;
import hello.model.User;

@Configuration
public class KafkaProducerConfig {

    private final Logger LOG = LoggerFactory.getLogger(getClass());

    @Value("${io.reflectoring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    /**
     * We are using StringSerializer for both keys and values.
     */
    // @Bean
    // public Map<String, Object> producerConfigs() {
    // Map<String, Object> props = new HashMap<>();
    // props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    // props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    // props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    // return props;
    // }

    /**
     * ProducerFactory is responsible for creating Kafka Producer instances
     * We are using StringSerializer for both keys and values
     */
    @Bean
    public ProducerFactory<String, String> producerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return new DefaultKafkaProducerFactory<>(props);
    }

    /**
     * KafkaTemplate helps us to send messages to their respective topic
     */
    // @Bean
    // public KafkaTemplate<String, String> kafkaTemplate() {
    // return new KafkaTemplate<>(producerFactory());
    // }

    /**
     * Configure KafkaTemplate with a ProducerListener which allows us to implement the onSuccess() and
     * onError() methods
     */
    @Bean
    KafkaTemplate<String, String> kafkaTemplate() {
        KafkaTemplate<String, String> kafkaTemplate = new KafkaTemplate<>(producerFactory());
        kafkaTemplate.setMessageConverter(new StringJsonMessageConverter());
        kafkaTemplate.setDefaultTopic("reflectoring-user");
        kafkaTemplate.setProducerListener(new ProducerListener<String, String>() {
            @Override
            public void onSuccess(ProducerRecord<String, String> producerRecord, RecordMetadata recordMetadata) {
                LOG.info("ACK from ProducerListener message: {} offset:  {}", producerRecord.value(),
                        recordMetadata.offset());
            }
        });
        return kafkaTemplate;
    }

    /**
     * We can use RoutingKafkaTemplate when we have multiple producers with different configurations and
     * we want to select producer at runtime based on the topic name
     * 
     * RoutingKafkaTemplate routes messages to the first ProducerFactory matching a given topic name.
     * If we have two patterns ref.* and reflectoring-.*, the pattern reflectoring-.* should be at the
     * beginning because the ref.* pattern would “override” it, otherwise.
     * 
     * In the below example, we have created two patterns .*-bytes and reflectoring-.*. The topic names
     * ending with ‘-bytes’ and starting with reflectoring-.* will use ByteArraySerializer and
     * StringSerializer respectively when we use RoutingKafkaTemplate instance.
     */
    @Bean
    public RoutingKafkaTemplate routingTemplate(GenericApplicationContext context) {
        // ProducerFactory with Bytes serializer
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        DefaultKafkaProducerFactory<Object, Object> bytesPF = new DefaultKafkaProducerFactory<>(props);
        context.registerBean(DefaultKafkaProducerFactory.class, "bytesPF", bytesPF);

        // ProducerFactory with String serializer
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        DefaultKafkaProducerFactory<Object, Object> stringPF = new DefaultKafkaProducerFactory<>(props);

        Map<Pattern, ProducerFactory<Object, Object>> map = new LinkedHashMap<>();
        map.put(Pattern.compile(".*-bytes"), bytesPF);
        map.put(Pattern.compile("reflectoring-.*"), stringPF);
        return new RoutingKafkaTemplate(map);
    }

    /**
     * ProducerFactory is responsible for creating Kafka Producer instances to send User object
     */
    @Bean
    public ProducerFactory<String, User> userProducerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        return new DefaultKafkaProducerFactory<>(configProps);
    }

    @Bean
    public KafkaTemplate<String, User> userKafkaTemplate() {
        return new KafkaTemplate<>(userProducerFactory());
    }
}
