package com.kareem.booking.model;

import lombok.*;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Document(collection = "outbox_events")
public class OutboxEvent {

    @Id
    private String id;

    @Indexed
    private String aggregateId;

    private String aggregateType;

    private String eventType;

    private String payload;

    @Indexed
    private OutboxStatus status;

    private Instant createdAt;

    private Instant publishedAt;

    private int retryCount;

    private String lastError;
}