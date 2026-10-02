package com.kareem.booking.dto;

import jakarta.validation.constraints.NotBlank;

public record BookingRequest(@NotBlank String slotId, @NotBlank String userId, @NotBlank String idempotencyKey) {}
