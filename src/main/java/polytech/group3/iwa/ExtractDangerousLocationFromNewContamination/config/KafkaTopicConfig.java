package polytech.group3.iwa.ExtractDangerousLocationFromNewContamination.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
class KafkaTopicConfig {

    @Bean
    public NewTopic locationTopic() {
        return TopicBuilder.name("location").config(TopicConfig.RETENTION_MS_CONFIG, "1680000").build();
    }

    @Bean
    public NewTopic contaminationTopic() {
        return TopicBuilder.name("covid_alert_db.public.covid_info").config(TopicConfig.RETENTION_MS_CONFIG, "1680000").build();
    }
    @Bean
    public NewTopic dangerousLocationTopic() {
        return TopicBuilder.name("dangerous_location").config(TopicConfig.RETENTION_MS_CONFIG, "1680000").build();
    }
}
