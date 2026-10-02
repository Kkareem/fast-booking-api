package com.kareem.booking.repository;

import com.kareem.booking.model.Booking;
import org.springframework.data.mongodb.repository.MongoRepository;

import java.util.Optional;

public interface BookingRepository extends MongoRepository<Booking, String> {
    Optional<Booking> findByIdempotencyKey(String idempotencyKey);
}
