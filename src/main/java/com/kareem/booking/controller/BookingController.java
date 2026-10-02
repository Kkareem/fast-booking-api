package com.kareem.booking.controller;

import com.kareem.booking.dto.*;
import com.kareem.booking.service.BookingService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController @RequestMapping("/api/bookings") @RequiredArgsConstructor
public class BookingController {
    private final BookingService service;

    @PostMapping("/before")
    public ResponseEntity<BookingResponse> before(@Valid @RequestBody BookingRequest request) { return ResponseEntity.ok(service.createBookingBefore(request)); }

    @PostMapping("/after")
    public ResponseEntity<BookingResponse> after(@Valid @RequestBody BookingRequest request) { return ResponseEntity.ok(service.createBookingAfter(request)); }
}
