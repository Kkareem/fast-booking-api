package com.kareem.booking.model;

import lombok.*;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;

@Data @Builder @NoArgsConstructor @AllArgsConstructor
@Document("bookings")
public class Booking {
    @Id private String id;
    private String slotId;
    private String userId;
    private String idempotencyKey;
    private String status;
    private Instant createdAt;
}
