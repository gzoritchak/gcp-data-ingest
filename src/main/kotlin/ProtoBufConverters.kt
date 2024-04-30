package org.example

import com.google.cloud.bigquery.storage.v1.BigDecimalByteStringEncoder
import com.google.cloud.bigquery.storage.v1.CivilTimeEncoder
import com.google.protobuf.ByteString
import kotlinx.datetime.Instant
import kotlinx.datetime.LocalDate
import kotlinx.datetime.LocalDateTime
import kotlinx.datetime.LocalTime
import java.math.BigDecimal

typealias TTLocalDateTime = org.threeten.bp.LocalDateTime
typealias TTLocalTime = org.threeten.bp.LocalTime

/**
 * Converts a string representation of a BigDecimal to a ByteString representation of a numeric value.
 */
fun String.bigDecimalToNumericByteString(): ByteString =
    BigDecimalByteStringEncoder.encodeToNumericByteString(BigDecimal(this))

/**
 * Converts a string representation of a BigDecimal to a ByteString representation of a big numeric value.
 */
fun String.bigDecimalToBigNumericByteString(): ByteString =
    BigDecimalByteStringEncoder.encodeToBigNumericByteString(BigDecimal(this))

/**
 * Converts the Instant object to a protocol buffer field value represented as a Long (millis).
 */
fun Instant.toProtoField(): Long = toEpochMilliseconds() * 1_000

/**
 * The local date is converted into an Int (epochDays)
 */
fun LocalDate.toProtoField(): Int = toEpochDays()

/**
 * Converts a LocalTime object to a protocol buffer field value.
 */
fun LocalTime.toProtoField()= CivilTimeEncoder.encodePacked64TimeMicros(this.toTTLocalTime())

/**
 * Converts a LocalDateTime object to a protocol buffer field value.
 */
fun LocalDateTime.toProtoField() = CivilTimeEncoder.encodePacked64DatetimeMicros(this.toTTLocalDateTime())

private fun LocalDateTime.toTTLocalDateTime(): TTLocalDateTime = TTLocalDateTime.of(year, monthNumber, dayOfMonth, hour, minute, second, nanosecond)
private fun LocalTime.toTTLocalTime(): TTLocalTime = TTLocalTime.of(hour, minute, second, nanosecond)
