package com.kareem.booking.repository;

import com.kareem.booking.model.Slot;
import org.springframework.data.mongodb.repository.MongoRepository;

public interface SlotRepository extends MongoRepository<Slot, String> {}
