package com.kareem.booking.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaConfig {
    @Bean NewTopic bookingEventsTopic() { return new NewTopic("booking-events", 3, (short) 1); }
}
