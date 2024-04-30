import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.example.chunked
import org.example.parallelProcessing
import org.junit.jupiter.api.Assertions.assertEquals
import org.slf4j.LoggerFactory
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.fail


private val testLogger  = LoggerFactory.getLogger("TestCoroutinesUtils")

class TestCoroutinesUtils {

    @Test
    fun `chunked without remaining items`() {
        runBlocking {
            val chunkedFlow = listOf(1, 2, 3, 4)
                .asFlow()
                .chunked(2)

            assertEquals(
                listOf(listOf(1, 2), listOf(3, 4)),
                chunkedFlow.toList()
            )
        }
    }

    @Test
    fun `chunked with remaining items`() {
        runBlocking {
            val chunkedFlow = listOf(1, 2, 3, 4, 5)
                .asFlow()
                .chunked(2)

            assertEquals(
                listOf(listOf(1, 2), listOf(3, 4), listOf(5)),
                chunkedFlow.toList()
            )
        }
    }


    @Test
    fun `parallel processing`() {
        runBlocking {
            val sum = AtomicInteger()
            (1..1000)
                .asFlow()
                .parallelProcessing {
                    testLogger.info("Processing $it")
                    sum.addAndGet(it)
                    delay(10)
                }
            assertEquals(500500, sum.get())
        }
    }

    @Test
    fun `parallel processing with error`() {
        val ret: Result<Unit> = runCatching {
            runBlocking {
                (1..101)
                    .asFlow()
                    .parallelProcessing {
                        if (it == 100) error("Error 100")
                    }
            }
        }
        if (ret.isFailure) {
            assertEquals("Error 100", ret.exceptionOrNull()?.message)
        } else {
            fail("Should have failed")
        }
    }
}