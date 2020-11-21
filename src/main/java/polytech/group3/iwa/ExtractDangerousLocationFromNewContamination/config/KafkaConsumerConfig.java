package polytech.group3.iwa.ExtractDangerousLocationFromNewContamination.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import polytech.group3.iwa.ExtractDangerousLocationFromNewContamination.models.ContaminationKafka;
import polytech.group3.iwa.ExtractDangerousLocationFromNewContamination.models.LocationKafka;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka
public class KafkaConsumerConfig {

    @Bean
    public Map<String, Object> consumerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put("bootstrap.servers", "localhost:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "json");

        return props;
    }

    @Bean
    public ConsumerFactory<String, LocationKafka> consumerFactoryLocation() {
        return new DefaultKafkaConsumerFactory<>(consumerConfigs(), new StringDeserializer(),
                new JsonDeserializer<>(LocationKafka.class));
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, LocationKafka> kafkaListenerContainerFactoryLocation() {
        ConcurrentKafkaListenerContainerFactory<String, LocationKafka> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactoryLocation());

        return factory;
    }

    @Bean
    public KafkaReceiverLocation receiverLocation() {
        return new KafkaReceiverLocation();
    }

    @Bean
    public ConsumerFactory<String, ContaminationKafka> consumerFactoryContamination() {
        return new DefaultKafkaConsumerFactory<>(consumerConfigs(), new StringDeserializer(),
                new JsonDeserializer<>(ContaminationKafka.class));
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, ContaminationKafka> kafkaListenerContainerFactoryContamination() {
        ConcurrentKafkaListenerContainerFactory<String, ContaminationKafka> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactoryContamination());

        return factory;
    }


}
