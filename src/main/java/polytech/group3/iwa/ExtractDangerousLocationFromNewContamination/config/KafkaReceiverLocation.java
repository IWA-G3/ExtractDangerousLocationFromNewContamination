package polytech.group3.iwa.ExtractDangerousLocationFromNewContamination.config;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import polytech.group3.iwa.ExtractDangerousLocationFromNewContamination.models.ContaminationKafka;
import polytech.group3.iwa.ExtractDangerousLocationFromNewContamination.models.LocationKafka;

import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.CountDownLatch;

@Component
@EnableKafka
class KafkaReceiverLocation {

    public List<LocationKafka> locationList = new ArrayList<LocationKafka>();
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaReceiverLocation.class);

    private CountDownLatch latch = new CountDownLatch(1);

    public KafkaReceiverLocation() {
        super();
    }

    public CountDownLatch getLatch() {
        return latch;
    }
    @KafkaListener(topics = "covid_alert_db.public.covid_info", containerFactory = "kafkaListenerContainerFactoryContamination")
    public void receive(
            @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key,
            @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
            @Header(KafkaHeaders.OFFSET) int offset) throws IOException {
        // Parse JSON object to retrieve id_keycloak
        ObjectMapper mapper = new ObjectMapper();
        JsonFactory factory = mapper.getFactory();
        JsonParser parser = factory.createParser(key);
        JsonNode payload = mapper.readTree(parser);
        String idKeycloak = mapper.writeValueAsString(payload.get("payload").get("id_keycloak"));
        LOGGER.info("received new contamination for user='{}'", idKeycloak);
        KafkaSender kafkaSender = new KafkaSender();
        // Fetch the locations of the contaminated user and insert them in the dangerous_location topic
        System.out.println(locationList.size());
        locationList.forEach(locationKafka -> {
            if(locationKafka.getUserid() == Integer.parseInt(idKeycloak))
                System.out.println("FOUND");
            kafkaSender.sendMessage(locationKafka, "dangerous_location");
        });
        latch.countDown();
    }
    @KafkaListener(
            groupId = "location",
            containerFactory = "kafkaListenerContainerFactoryLocation",
            topicPartitions = @TopicPartition(
                    topic = "location",
                    partitionOffsets = { @PartitionOffset(
                            partition = "0",
                            initialOffset = "0") }))
    void listenToPartitionWithOffset(
            @Payload LocationKafka message,
            @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
            @Header(KafkaHeaders.OFFSET) int offset) {
        LOGGER.info("Received location [{}] from partition-{} with offset-{}",
                message,
                partition,
                offset);
        locationList.add(message);
        int i = 0;
        while(i < locationList.size() && Duration.between(LocalDateTime.parse(locationList.get(i).getLocation_date(), DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss")), LocalDateTime.now()).toHours() > 72) {
            locationList.remove(i);
        };
        System.out.println("there are " + locationList.size() +  " locations");
        latch.countDown();
    }
}