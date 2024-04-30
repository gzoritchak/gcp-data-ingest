package bigquery

import com.google.cloud.bigquery.*
import com.google.cloud.bigquery.storage.v1.ProtoRows
import com.google.protobuf.DynamicMessage
import kotlinx.coroutines.runBlocking
import kotlinx.datetime.Instant
import kotlinx.datetime.TimeZone
import kotlinx.datetime.toLocalDateTime
import org.example.*
import org.junit.jupiter.api.Test


class TestInsertProtobuf {

    @Test
    fun testInsertProtobuf() {
        attenuateGoogleLogs()
        val bigQuery  = bigQuery(projectId)
        bigQuery.createDatasetIfNeeded(datasetName)

        val schema: Schema =  Schema.of(
            listOf(
                Field.of("Boolean",     LegacySQLTypeName.valueOf("Boolean")),
                Field.of("Date",        LegacySQLTypeName.valueOf("Date")),
                Field.of("Datetime",    LegacySQLTypeName.valueOf("Datetime")),
                Field.of("Int64",       LegacySQLTypeName.valueOf("Int64")),
                Field.of("Numeric",     LegacySQLTypeName.valueOf("Numeric")),
                Field.of("BigNumeric",  LegacySQLTypeName.valueOf("BigNumeric")),
                Field.of("Float64",     LegacySQLTypeName.valueOf("Float64")),
                Field.of("String",      LegacySQLTypeName.valueOf("String")),
                Field.of("Time",        LegacySQLTypeName.valueOf("Time")),
                Field.of("Timestamp",   LegacySQLTypeName.valueOf("Timestamp")),
            ))

        val builder = StandardTableDefinition
            .newBuilder()
            .setSchema(schema)

        val tableInfo: TableInfo = TableInfo.of(
            TableId.of(datasetName, "insert_protobuf"),
            builder.build()
        )
        try {
            bigQuery.create(tableInfo)
        } catch (e: Exception) {
            println(e.message)
        }

        val instant = Instant.parse("2024-02-28T23:12:59.123Z")
        val localDateTime = instant.toLocalDateTime(TimeZone.UTC)

        val tableId = TableId.of(projectId, datasetName, "insert_protobuf")
        ProtoWriter(tableId).use { protoWriter ->
            val message: DynamicMessage.Builder = DynamicMessage.newBuilder(protoWriter.descriptor)
            message.setField(protoWriter.descriptor.fields[0], true)
            message.setField(protoWriter.descriptor.fields[1], localDateTime.date.toProtoField())
            message.setField(protoWriter.descriptor.fields[2], localDateTime.toProtoField())
            message.setField(protoWriter.descriptor.fields[3], Long.MAX_VALUE)
            message.setField(protoWriter.descriptor.fields[4], "12345678901234567890.123456789".bigDecimalToNumericByteString())
            message.setField(protoWriter.descriptor.fields[5], "123456789012345678901234567890.012345678".bigDecimalToBigNumericByteString())
            message.setField(protoWriter.descriptor.fields[6], 1234567890.0123456)
            message.setField(protoWriter.descriptor.fields[7], "une chaîne de caractères avec un \n retour à la ligne")
            message.setField(protoWriter.descriptor.fields[8], localDateTime.time.toProtoField())
            message.setField(protoWriter.descriptor.fields[9], instant.toProtoField())
            val protoRows = ProtoRows.newBuilder().addSerializedRows(message.build().toByteString())
            runBlocking {
                try {
                    val response = protoWriter.appendRows(protoRows.build())
                    response
                } catch (e: Exception) {
                    e.printStackTrace()
                }
            }
        }
    }
}

