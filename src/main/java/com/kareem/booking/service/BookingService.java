package com.kareem.booking.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kareem.booking.dto.BookingRequest;
import com.kareem.booking.dto.BookingResponse;
import com.kareem.booking.model.Booking;
import com.kareem.booking.model.OutboxEvent;
import com.kareem.booking.model.OutboxStatus;
import com.kareem.booking.model.Slot;
import com.kareem.booking.repository.BookingRepository;
import com.kareem.booking.repository.OutboxEventRepository;
import com.kareem.booking.repository.SlotRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class BookingService {
    private final BookingRepository bookingRepository;
    private final SlotRepository slotRepository;
    private final MongoTemplate mongoTemplate;
    private final StringRedisTemplate redis;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

    /**
     * Portfolio: intentionally naive implementation used as the BEFORE baseline.
     */
    public BookingResponse createBookingBefore(BookingRequest request) {
        var existing = bookingRepository.findByIdempotencyKey(request.idempotencyKey());
        if (existing.isPresent()) return toResponse(existing.get());
        Slot slot = slotRepository.findById(request.slotId()).orElseThrow();
        if (slot.getBooked() >= slot.getCapacity()) throw new IllegalStateException("SLOT_FULL");
        slot.setBooked(slot.getBooked() + 1);
        slotRepository.save(slot);
        Booking booking = bookingRepository.save(Booking.builder()
                .id(UUID.randomUUID().toString()).slotId(request.slotId()).userId(request.userId())
                .idempotencyKey(request.idempotencyKey()).status("CONFIRMED").createdAt(Instant.now()).build());
        return toResponse(booking);
    }

    /**
     * Portfolio: atomic capacity reservation + idempotency + failure compensation + outbox event.
     */
    public BookingResponse createBookingAfter(BookingRequest request) {

        String idemKey = "booking:idem:" + request.idempotencyKey();

        // 1. Redis fast path
        String cached = redis.opsForValue().get(idemKey);

        if (cached != null) {
            return new BookingResponse(
                    cached,
                    request.slotId(),
                    request.userId(),
                    "CONFIRMED"
            );
        }

        // 2. Durable idempotency check
        Booking existing = bookingRepository
                .findByIdempotencyKey(request.idempotencyKey())
                .orElse(null);

        if (existing != null) {
            redis.opsForValue().set(
                    idemKey,
                    existing.getId(),
                    Duration.ofHours(24)
            );

            return toResponse(existing);
        }

        boolean capacityReserved = false;

        try {

            // 3. Atomically reserve capacity
            Query query = Query.query(
                    Criteria.where("_id")
                            .is(request.slotId())
                            .and("$expr")
                            .is(
                                    new org.bson.Document(
                                            "$lt",
                                            java.util.List.of(
                                                    "$booked",
                                                    "$capacity"
                                            )
                                    )
                            )
            );

            Update update = new Update()
                    .inc("booked", 1);

            Slot updated = mongoTemplate.findAndModify(
                    query,
                    update,
                    org.springframework.data.mongodb.core
                            .FindAndModifyOptions
                            .options()
                            .returnNew(true),
                    Slot.class
            );

            if (updated == null) {
                throw new IllegalStateException("SLOT_FULL");
            }

            capacityReserved = true;

            // 4. Persist booking
            Booking booking = bookingRepository.save(
                    Booking.builder()
                            .id(UUID.randomUUID().toString())
                            .slotId(request.slotId())
                            .userId(request.userId())
                            .idempotencyKey(request.idempotencyKey())
                            .status("CONFIRMED")
                            .createdAt(Instant.now())
                            .build()
            );

            OutboxEvent event = OutboxEvent.builder()
                    .id(UUID.randomUUID().toString())
                    .aggregateId(booking.getId())
                    .aggregateType("BOOKING")
                    .eventType("BOOKING_CONFIRMED")
                    .payload(toJson(booking))
                    .status(OutboxStatus.PENDING)
                    .createdAt(Instant.now())
                    .retryCount(0)
                    .build();

            outboxEventRepository.save(event);

            // Booking is durable now.
            // Do NOT rollback capacity for Redis/Kafka failures.
            capacityReserved = false;

            // 5. Redis cache
            redis.opsForValue().set(
                    idemKey,
                    booking.getId(),
                    Duration.ofHours(24)
            );

            return toResponse(booking);

        } catch (DuplicateKeyException e) {

            if (capacityReserved) {
                releaseCapacity(request.slotId());
            }

            Booking existingBooking = bookingRepository
                    .findByIdempotencyKey(request.idempotencyKey())
                    .orElseThrow(() -> e);

            redis.opsForValue().set(
                    idemKey,
                    existingBooking.getId(),
                    Duration.ofHours(24)
            );

            return toResponse(existingBooking);

        } catch (RuntimeException e) {

            if (capacityReserved) {
                releaseCapacity(request.slotId());
            }

            throw e;
        }
    }

    private void releaseCapacity(String slotId) {

        Query query = Query.query(
                Criteria.where("_id")
                        .is(slotId)
                        .and("booked")
                        .gt(0)
        );

        Update update = new Update()
                .inc("booked", -1);

        mongoTemplate.updateFirst(
                query,
                update,
                Slot.class
        );
    }

    private String toJson(Booking booking) {
        try {
            return objectMapper.writeValueAsString(booking);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(
                    "FAILED_TO_SERIALIZE_BOOKING_EVENT",
                    e
            );
        }
    }

    private BookingResponse toResponse(Booking b) {
        return new BookingResponse(b.getId(), b.getSlotId(), b.getUserId(), b.getStatus());
    }
}
