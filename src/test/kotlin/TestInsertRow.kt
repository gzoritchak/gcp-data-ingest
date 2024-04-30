import com.google.cloud.bigquery.storage.v1.ProtoRows
import kotlinx.coroutines.runBlocking
import kotlinx.datetime.Clock
import kotlinx.datetime.LocalDate
import org.example.*
import kotlin.test.Test


class TestInsertRow {

    @Test
    fun testInsertRow() {
        val writer = tableId.protoWriter()
        val row = writer.insertTestRow(
            "test",
            1234L,
            LocalDate.parse("2024-04-30"),
            "12345.6789".toBigDecimal(),
            "no_file.txt",
            Clock.System.now().toProtoField()
            )
        val rows = ProtoRows.newBuilder()
        rows.addSerializedRows(row)
        runBlocking {
            writer.appendRows(rows.build())
        }
        writer.close()
    }
}