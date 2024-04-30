import com.google.cloud.bigquery.*
import org.example.datasetName
import org.example.projectId
import org.example.bigQuery
import org.example.tableName


fun main() {
    bigQuery(projectId)
        .createTable(datasetName, tableName) {
            colums {
                col("A", LegacySQLTypeName.STRING)
                col("B", LegacySQLTypeName.INTEGER)
                col("C", LegacySQLTypeName.DATE)
                col("D", LegacySQLTypeName.NUMERIC)
                col("_filename", LegacySQLTypeName.STRING)
                col("_inserted_at", LegacySQLTypeName.TIMESTAMP)
            }
        }

}

fun BigQuery.createTable(dataset: String, tableName:String, build: TableBuilder.() -> Unit) {
    val builder = TableBuilder().apply(build)
    val tableDefinition = StandardTableDefinition
        .newBuilder()
        .setSchema(
            Schema.of(
                builder.columns.map {
                    Field.of(it.first, it.second)
                })
        ).build()

    create(
        TableInfo.of(
            TableId.of(dataset, tableName),
            tableDefinition
        )
    )

    println("Table created $dataset:$tableName")
}


/**
 * Small DSL for table creation
 */
class TableBuilder {
    val columns = mutableListOf<Pair<String, LegacySQLTypeName>>()
    class ColBuilder(val tableBuilder: TableBuilder) {
        fun col(name: String, type: LegacySQLTypeName) {
            tableBuilder.columns.add(name to type)
        }
    }
    val colBuilder = ColBuilder(this)
    fun colums(init: ColBuilder.() -> Unit) {
        colBuilder.init()
    }
}