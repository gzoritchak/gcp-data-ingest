package org.example

import kotlinx.datetime.Instant
import kotlinx.datetime.LocalDate
import kotlinx.datetime.LocalDateTime
import org.threeten.bp.ZoneOffset
import org.threeten.bp.format.DateTimeFormatter
import org.threeten.bp.format.DateTimeFormatterBuilder
import org.threeten.bp.format.TextStyle
import org.threeten.bp.temporal.ChronoField
import org.threeten.bp.temporal.TemporalAccessor
import java.math.BigDecimal
import java.math.RoundingMode


/**
 * Converts a string to a BigDecimal, with a scale of 9 (biggest scale accepted by BigQuery)
 */
fun String.toBigDecimal(): BigDecimal = BigDecimal(this)
    .setScale(9, RoundingMode.HALF_EVEN)

/**
 * Converts a CSV string to a Boolean value, accepting true, false, 1, 0
 *
 * @return the corresponding Boolean value of the CSV string
 * @throws IllegalArgumentException if the CSV string cannot be parsed to Boolean
 */
fun String.csvToBoolean(): Boolean = when (this.lowercase()) {
    "0", "false" -> false
    "1", "true" -> true
    else -> error("$this can't be parsed to Boolean")
}

/**
 * Converts this string representing a timestamp to an Instant
 */
fun String.timestampToInstant(): Instant = timestampAccessor().toInstant()

/**
 *
 */
fun String.timestampAccessor(): TemporalAccessor = timestampFormatter.parse(this)

/**
 * @return The `Instant` equivalent of the `TemporalAccessor` instance.
 */
fun TemporalAccessor.toInstant(): Instant =
    Instant.fromEpochMilliseconds(
        getLong(ChronoField.INSTANT_SECONDS) * 1_000 + getLong(ChronoField.MILLI_OF_SECOND)
    )



/**
 * Convert a String to a LocalDateTime.
 */
fun String.toLocalDate(): LocalDate = localDateFormatter.parse(this).toLocalDate()

/**
 * Convert a String to a LocalDateTime.
 */
fun String.toLocalDateTime(): LocalDateTime = timestampFormatter.parse(this).toLocalDateTime()

/**
 * @return The `Instant` equivalent of the `TemporalAccessor` instance. Only YEAR, MONTH and DAY are taken.
 */
fun TemporalAccessor.toLocalDate(): LocalDate =
    LocalDate(
        get(ChronoField.YEAR),
        get(ChronoField.MONTH_OF_YEAR),
        get(ChronoField.DAY_OF_MONTH),
    )

/**
 * @return The `Instant` equivalent of the `TemporalAccessor` instance.
 */
fun TemporalAccessor.toLocalDateTime(): LocalDateTime =
    LocalDateTime(
        get(ChronoField.YEAR),
        get(ChronoField.MONTH_OF_YEAR),
        get(ChronoField.DAY_OF_MONTH),
        get(ChronoField.HOUR_OF_DAY),
        get(ChronoField.MINUTE_OF_HOUR),
        get(ChronoField.SECOND_OF_MINUTE),
//        get(ChronoField.NANO_OF_SECOND),
        // todo decimals of seconds removed to stick with legacy dataflow implementation, should we put it back?
    )

/**
 * A permissive date formatter used for parsing and formatting date and time strings in a specific format.
 * The format follows the pattern "yyyy-MM-dd['T'][ ][[HH:mm:ss.SSSSSS]]",
 * where each component is optional.
 */
val localDateFormatter =
    DateTimeFormatterBuilder()
        .parseLenient()
        .append(DateTimeFormatter.ofPattern("yyyy[/][-]MM[/][-]dd"))
        .optionalStart()
        .appendLiteral('T')
        .optionalEnd()
        .optionalStart()
        .appendLiteral(' ')
        .optionalEnd()
        .optionalStart()
        .appendValue(ChronoField.HOUR_OF_DAY, 2)
        .appendLiteral(':')
        .appendValue(ChronoField.MINUTE_OF_HOUR, 2)
        .optionalStart()
        .appendLiteral(':')
        .appendValue(ChronoField.SECOND_OF_MINUTE, 2)
        .optionalEnd()
        .optionalStart()
        .appendValue(ChronoField.MILLI_OF_SECOND, 3)
        .optionalEnd()
        .optionalStart()
        .appendFraction(ChronoField.MICRO_OF_SECOND, 3, 6, true)
        .optionalEnd()
        .optionalStart()
        .appendFraction(ChronoField.NANO_OF_SECOND, 6, 9, true)
        .optionalEnd()
        .optionalEnd()
        .toFormatter()

/**
 * DateTimeFormatter for formatting and parsing LocalDateTime objects.
 *
 * The formatter is configured to handle the following patterns:
 * - "yyyy-MM-dd"
 * - "yyyy/MM/dd"
 * - "yyyy-MM-dd'T'HH:mm"
 * - "yyyy/MM/dd HH:mm"
 * - "yyyy-MM-dd'T'HH:mm:ss"
 * - "yyyy/MM/dd HH:mm:ss"
 * - "yyyy-MM-dd'T'HH:mm:ss.SSS"
 * - "yyyy/MM/dd HH:mm:ss.SSS"
 * - "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"
 * - "yyyy/MM/dd HH:mm:ss.SSSSSS"
 * - "yyyy-MM-dd'T'HH:mm:ss.nnnnnnnnn"
 * - "yyyy/MM/dd HH:mm:ss.nnnnnnnnn"
 *
 * Example usage:
 * ```
 * val formattedDateTime = localDateTimeFormatter.format(LocalDateTime.now())
 * val parsedDateTime = localDateTimeFormatter.parse("2022-06-28T14:30:00.123456789")
 * ```
 */
val localDateTimeFormatter: DateTimeFormatter =
    DateTimeFormatterBuilder()
        .parseLenient()
        .append(DateTimeFormatter.ofPattern("yyyy[/][-]MM[/][-]dd"))
        .optionalStart()
        .appendLiteral('T')
        .optionalEnd()
        .optionalStart()
        .appendLiteral(' ')
        .optionalEnd()
        .appendValue(ChronoField.HOUR_OF_DAY, 2)
        .appendLiteral(':')
        .appendValue(ChronoField.MINUTE_OF_HOUR, 2)
        .optionalStart()
        .appendLiteral(':')
        .appendValue(ChronoField.SECOND_OF_MINUTE, 2)
        .optionalEnd()
        .optionalStart()
        .appendValue(ChronoField.MILLI_OF_SECOND, 3)
        .optionalEnd()
        .optionalStart()
        .appendFraction(ChronoField.MICRO_OF_SECOND, 3, 6, true)
        .optionalEnd()
        .optionalStart()
        .appendFraction(ChronoField.NANO_OF_SECOND, 6, 9, true)
        .optionalEnd()
        .toFormatter()

/**
 * Formatter to parse and format timestamps in different formats.
 * The formatter is built using DateTimeFormatterBuilder and supports the following patterns:
 */
internal val timestampFormatter: DateTimeFormatter =
    DateTimeFormatterBuilder()
        .parseLenient()
        .append(DateTimeFormatter.ofPattern("yyyy[/][-]MM[/][-]dd"))
        .optionalStart()
        .appendLiteral('T')
        .optionalEnd()
        .optionalStart()
        .appendLiteral(' ')
        .optionalEnd()
        .appendValue(ChronoField.HOUR_OF_DAY, 2)
        .appendLiteral(':')
        .appendValue(ChronoField.MINUTE_OF_HOUR, 2)
        .optionalStart()
        .appendLiteral(':')
        .appendValue(ChronoField.SECOND_OF_MINUTE, 2)
        .optionalEnd()
        .optionalStart()
        .appendValue(ChronoField.MILLI_OF_SECOND, 3)
        .optionalEnd()
        .optionalStart()
        .appendFraction(ChronoField.MICRO_OF_SECOND, 3, 6, true)
        .optionalEnd()
        .optionalStart()
        .appendFraction(ChronoField.NANO_OF_SECOND, 6, 9, true)
        .optionalEnd()
        .optionalStart()
        .appendLiteral(' ')
        .optionalEnd()
        .optionalStart()
        .appendOffset("+HH:MM", "+00:00")
        .optionalEnd()
        .optionalStart()
        .appendZoneText(TextStyle.SHORT)
        .optionalEnd()
        .optionalStart()
        .appendLiteral('Z')
        .optionalEnd()
        .toFormatter()
        .withZone(ZoneOffset.UTC)
