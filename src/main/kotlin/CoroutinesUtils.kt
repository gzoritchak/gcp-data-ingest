package org.example

import com.google.api.core.ApiFuture
import com.google.api.core.ApiFutureCallback
import com.google.api.core.ApiFutures
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.flow
import java.util.ArrayList
import kotlin.time.Duration
import kotlin.time.Duration.Companion.nanoseconds
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.time.TimeSource

/**
 * Collect all elements of the flow and send them to a Channel for the parallel
 * execution of the given block.
 *
 * The size of the Channel is given by the parallelism parameter.
 */
suspend fun <T> Flow<T>.parallelProcessing(
    parallelism: Int = 20,
    processElement: suspend (T) -> Unit
) {
    // Max RAM overhead = memory_size_of(T) × parallelism × 2
    withContext(Dispatchers.Default) {
        val inputChannel = Channel<T>(parallelism)
        launch {
            collect {
                inputChannel.send(it)
            }
            inputChannel.close()
        }
        for (i in 0..<parallelism) launch {
            for (element in inputChannel) {
                processElement(element)
            }
        }
    }
}

/**
 * Convert a flow of T into a flow of List<T>.
 * Naturally, the last element of the flow can have a size smaller than the
 * requested chunk size.
 * @param size the size of the chunks
 */
fun <T> Flow<T>.chunked(size: Int): Flow<List<T>> = flow {
    val elements = ArrayList<T>(size)
    collect {
        elements.add(it)
        if (elements.size == size) {
            emit(elements.toList())
            elements.clear()
        }
    }
    if (elements.isNotEmpty())
        emit(elements)
}

/**
 * Adapter to convert an ApiFuture API in a suspendable function
 */
suspend fun <T> ApiFuture<T>.await(dispatcher: CoroutineDispatcher = Dispatchers.Default ): T =
    suspendCancellableCoroutine { cancellableContinuation ->
        val callback = object : ApiFutureCallback<T> {
            override fun onFailure(t: Throwable) = cancellableContinuation.resumeWithException(t)
            override fun onSuccess(result: T) = cancellableContinuation.resume(result)
        }
        ApiFutures.addCallback(this, callback, dispatcher.asExecutor())
    }
