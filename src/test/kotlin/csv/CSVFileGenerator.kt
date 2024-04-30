package csv

import DataType.DataType
import DataType.DataType.STRING
import java.io.File
import java.io.OutputStreamWriter
import java.io.Writer
import kotlin.random.Random

/**
 * Generate a CSV file for testing.
 */
fun main() {
    generate(1000)
}


val defaultSchema = Schema(
    listOf(
        Field("A", STRING),
        Field("B", DataType.INTEGER),
        Field("C", DataType.DATE),
        Field("D", DataType.NUMERIC)
    )
)

class Schema(val fields: List<Field>) {

}

class Field(
    val name: String,
    val type: Any) {

}

fun generate(sizeInMiB: Int, schema: Schema = defaultSchema) {
//    val schemaJson = Json.encodeToString(schema)
//    File("generate.json").writeText(schemaJson)
//

    fun addLine() = schema.randomLine()

    val output = File("generated_${sizeInMiB}MB.csv")
    val writer = CountWriter(output.writer())
    writer.use {
        while (writer.size < sizeInMiB * 1024 * 1024) {
            it.appendLine(addLine())
        }
    }

}

class CountWriter(private val delegate: OutputStreamWriter): Writer() {

    var size = 0

    override fun write(cbuf: CharArray, off: Int, len: Int) {
        delegate.write(cbuf, off, len)
        size += len
    }

    override fun close() = delegate.close()
    override fun flush() = delegate.flush()

}



fun Schema.randomLine() =
    fields.joinToString(";")
        { it.randomContent() }

val random = Random(42)

fun Field.randomContent() = when (this.type) {

    STRING -> "abcdefghijklmnopqrstuvxyz"

//    """"abc
//        |defghijklmnopqrstuvwxyz
//    """".trimMargin()

    DataType.INTEGER -> random.nextInt().toString()
    DataType.NUMERIC -> random.nextDouble().toString()
    DataType.DATE -> "2023-01-12"

    else -> error("No generation for $type")
}