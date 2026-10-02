package com.kareem.booking.repository;

import com.kareem.booking.model.OutboxEvent;
import com.kareem.booking.model.OutboxStatus;
import org.springframework.data.mongodb.repository.MongoRepository;

import java.util.List;

public interface OutboxEventRepository
        extends MongoRepository<OutboxEvent, String> {

    List<OutboxEvent> findTop100ByStatusOrderByCreatedAtAsc(
            OutboxStatus status
    );
}