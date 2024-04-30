package org.example

import com.google.cloud.storage.Blob
import com.google.cloud.storage.BlobId
import com.google.cloud.storage.Storage
import com.google.cloud.storage.StorageOptions
import java.io.BufferedReader
import java.io.InputStreamReader
import java.nio.channels.Channels
import java.nio.channels.ReadableByteChannel
import java.util.zip.ZipInputStream

/**
 * Creates a new storage instance with the specified project ID.
 */
fun Storage(projetId: String): Storage = StorageOptions.newBuilder()
    .setCredentials(defaultCredentials)
    .setProjectId(projetId)
    .build()
    .service

/**
 * @return a reference to the blob
 */
fun Storage.getBlob(bucket: String, name: String): Blob = get(BlobId.of(bucket, name)) ?: error(
    "No blob for $bucket $name" +
            if (name.startsWith("/")) "\n The name shouldn't start with a '/'. Try removing it." else ""
)

/**
 * Returns a buffered reader from a blob.
 * If the name ends with .zip, unzip the content.
 */
fun Blob.bufferedReader() =
    if (name.endsWith(".zip")) //use Content-type?
        reader().zippedChannelToReader()
    else
        BufferedReader(Channels.newReader(reader(), "UTF-8"))

/**
 * Converts a zipped channel to a Reader
 */
fun ReadableByteChannel.zippedChannelToReader(): BufferedReader {
    val zipInputStream = ZipInputStream(Channels.newInputStream(this))
        .also { it.nextEntry }
    return BufferedReader(InputStreamReader(zipInputStream))
}
