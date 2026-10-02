package com.kareem.booking.model;

import lombok.*;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

@Data @Builder @NoArgsConstructor @AllArgsConstructor
@Document("slots")
public class Slot {
    @Id private String id;
    private int capacity;
    private int booked;
    private String version;
}
