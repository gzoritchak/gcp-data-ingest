package org.example

import com.google.cloud.bigquery.TableId
import com.google.cloud.bigquery.storage.v1.BigDecimalByteStringEncoder
import com.google.cloud.bigquery.storage.v1.ProtoRows
import com.google.protobuf.ByteString
import com.google.protobuf.DynamicMessage
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.count
import kotlinx.datetime.Clock
import kotlinx.datetime.LocalDate
import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVRecord
import org.slf4j.LoggerFactory
import java.math.BigDecimal

private val logger = LoggerFactory.getLogger("Loader")!!

const val bucketName    = "d2v-ingest-demo"
const val fileName      = "generated_200MiB.csv"

const val projectId     = "you-project-id"
const val datasetName   = "ingest_demo"
const val tableName     = "insert_test"

val tableId = TableId.of(projectId, datasetName, tableName)!!

fun main()  = runBlocking {
    attenuateGoogleLogs()
    val reader = Storage(projectId)
        .getBlob(bucketName, fileName)
        .also { logger.info("${it.size / 1024 / 1024} MiB") }
        .bufferedReader()

    val stats = StatisticCollector(logger)
    val writer = tableId.protoWriter()
    val now = Clock.System.now().toProtoField()

    csvFormat
        .parse(reader)
        .asFlow()
        .chunked(2000)
        .parallelProcessing {
            stats.addIngestedRows(it.size)
            val beforeProcess  = System.nanoTime()
            val rowsBuilder = ProtoRows.newBuilder()
            for (record in it) {
                rowsBuilder.addSerializedRows(record.toProtoMessage(writer, fileName, now))
            }
            val rows = rowsBuilder.build()
            val afterProcess = System.nanoTime()
            writer.appendRows(rows)
            val afterRequest = System.nanoTime()
            stats.addProcessDuration(afterProcess-beforeProcess)
            stats.addRequestDuration(afterRequest - afterProcess)
        }
    stats.logStats()
    writer.close()
    return@runBlocking
}











val csvFormat: CSVFormat = CSVFormat.Builder.create(CSVFormat.DEFAULT)
    .setIgnoreSurroundingSpaces(true)
    .setDelimiter(';')
    .build()


/**
 * Converts the CSVRecord to a ProtoBuf message
 */
fun CSVRecord.toProtoMessage(
    writer: ProtoWriter,
    fileName: String,
    now: Long,
): ByteString = writer.insertTestRow(
    get(0),
    get(1).toLong(),
    get(2).toLocalDate(),
    get(3).toBigDecimal(),
    fileName,
    now)

/**
 * Builds the protobuf message for row insertion
 */
fun ProtoWriter.insertTestRow (
    a: String, b: Long, c: LocalDate, d: BigDecimal, fileName: String, now: Long): ByteString =
    DynamicMessage.newBuilder(descriptor)
        .apply {
            setField(descriptor.fields[0], a)
            setField(descriptor.fields[1], b)
            setField(descriptor.fields[2], c.toProtoField())
            setField(descriptor.fields[3], BigDecimalByteStringEncoder.encodeToNumericByteString(d))
            setField(descriptor.fields[4], fileName)
            setField(descriptor.fields[5], now)

        }.build().toByteString()
