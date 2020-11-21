package polytech.group3.iwa.ExtractDangerousLocationFromNewContamination.config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.stereotype.Component;
import polytech.group3.iwa.ExtractDangerousLocationFromNewContamination.models.LocationKafka;

import java.util.HashMap;
import java.util.Map;

@Component
@Configuration
public class KafkaSender {

    public Map<String, Object> producerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, false);
        props.put("bootstrap.servers", "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        return props;
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSender.class);

    private KafkaTemplate<String, LocationKafka> kafkaTemplate;

    @Autowired
    public KafkaSender() {
        this.kafkaTemplate = kafkaTemplate = new KafkaTemplate<String, LocationKafka>(new DefaultKafkaProducerFactory<>(producerConfigs()));
    }

    public void sendMessage(LocationKafka message, String topicName) {
        LOGGER.info("sending message='{}'", message.toString());
        kafkaTemplate.send(topicName, message);
    }
}