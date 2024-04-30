package org.example

import com.google.cloud.bigquery.TableId
import com.google.cloud.bigquery.storage.v1.*
import com.google.protobuf.Descriptors
import java.io.Closeable
import java.util.concurrent.TimeUnit

/**
 * Util extension function to simplify the creation of a ProtoWriter on
 * the table.
 */
fun TableId.protoWriter() = ProtoWriter(this)

/**
 * This class in only responsible for sending protobuf message through
 * Writer Storage API using _default stream.
 */
class ProtoWriter(
    tableId: TableId,
): Closeable {

    val descriptor: Descriptors.Descriptor

    private val streamWriter: StreamWriter
    private val client: BigQueryWriteClient = BigQueryWriteClient()

    init {
        val parentTable: TableName = TableName.of(
            tableId.project,
            tableId.dataset,
            tableId.table
        )

        val newWriteStream = WriteStream.newBuilder()
            .setType(WriteStream.Type.COMMITTED)
            .build()

        val createWriteStreamRequest = CreateWriteStreamRequest.newBuilder()
            .setParent(parentTable.toString())
            .setWriteStream(newWriteStream)
            .build()

        val writeStream = client.createWriteStream(createWriteStreamRequest)

        descriptor = BQTableSchemaToProtoDescriptor.convertBQTableSchemaToProtoDescriptor(writeStream.tableSchema)
        streamWriter = StreamWriter.newBuilder(
            "projects/${tableId.project}/datasets/${tableId.dataset}/tables/${tableId.table}/streams/_default",
            client
        )
            .setWriterSchema(
                ProtoSchemaConverter.convert(
                    BQTableSchemaToProtoDescriptor.convertBQTableSchemaToProtoDescriptor(
                        writeStream.tableSchema
                    )
                )
            )
            .build()
    }

    /**
     * Performs the suspend call on bigquery write api
     */
    suspend fun appendRows(rows: ProtoRows): AppendRowsResponse =
        streamWriter.append(rows).await()

    override fun close() {
        streamWriter.close()
        client.shutdown()
        client.close()
        client.awaitTermination(5, TimeUnit.SECONDS)
    }

}

