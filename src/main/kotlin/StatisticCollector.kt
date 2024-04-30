package org.example

import org.slf4j.Logger
import java.util.concurrent.atomic.AtomicLong
import kotlin.time.Duration.Companion.nanoseconds

class StatisticCollector(private val logger: Logger) {
    private val start = System.nanoTime()
    private val processDuration = AtomicLong()
    private val requestDuration = AtomicLong()
    private val ingestedRows = AtomicLong()

    fun addIngestedRows(size: Int) {
        ingestedRows.addAndGet(size.toLong())
    }

    fun logStats() {
        val total = System.nanoTime() - start
        logger.info("Ingested ${ingestedRows.get()} rows in ${total.nanoseconds}")
        logger.info("Process duration: ${processDuration.get().nanoseconds}  parallelization ${(processDuration.get().toFloat() / total)}")
        logger.info("Request duration: ${requestDuration.get().nanoseconds}  parallelization ${(requestDuration.get().toFloat() / total)}")

    }

    fun addProcessDuration(duration: Long) {
        processDuration.addAndGet(duration)

    }

    fun addRequestDuration(duration: Long) {
        requestDuration.addAndGet(duration)
    }

}