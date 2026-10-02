package com.kareem.booking.service;

import com.kareem.booking.model.OutboxEvent;
import com.kareem.booking.model.OutboxStatus;
import com.kareem.booking.repository.OutboxEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class OutboxPublisher {

    private static final int MAX_RETRIES = 5;

    private final OutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Scheduled(fixedDelay = 1000)
    public void publishPendingEvents() {

        List<OutboxEvent> events =
                outboxEventRepository
                        .findTop100ByStatusOrderByCreatedAtAsc(
                                OutboxStatus.PENDING
                        );

        for (OutboxEvent event : events) {
            publish(event);
        }
    }

    private void publish(OutboxEvent event) {

        try {

            kafkaTemplate.send(
                    "booking-events",
                    event.getAggregateId(),
                    event.getPayload()
            ).get();

            event.setStatus(OutboxStatus.PUBLISHED);
            event.setPublishedAt(Instant.now());
            event.setLastError(null);

            outboxEventRepository.save(event);

        } catch (Exception e) {

            int retries = event.getRetryCount() + 1;

            event.setRetryCount(retries);
            event.setLastError(e.getMessage());

            if (retries >= MAX_RETRIES) {
                event.setStatus(OutboxStatus.FAILED);
            }

            outboxEventRepository.save(event);

            log.error(
                    "Failed to publish outbox event id={}, retry={}",
                    event.getId(),
                    retries,
                    e
            );
        }
    }
}