package hello.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * We can also use the @KafkaListener annotation at class level. If we do so, we need to
 * specify @KafkaHandler at the method level
 * 
 * Below example, messages of type String will be received by listen() and type Object will be
 * received by listenDefault(). Whenever there is no match, the default handler (defined by
 * isDefault=true) will be called
 */
@Component
@KafkaListener(id = "class-level", topics = "reflectoring-1")
public class KafkaClassListener {
    private final Logger LOG = LoggerFactory.getLogger(KafkaListenersExample.class);

    @KafkaHandler
    public void listen(String message) {
        LOG.info("KafkaHandler[String] {}", message);
    }

    @KafkaHandler(isDefault = true)
    public void listenDefault(Object object) {
        LOG.info("KafkaHandler[Default] {}", object);
    }
}
