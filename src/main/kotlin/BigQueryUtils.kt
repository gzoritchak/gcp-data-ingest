package org.example

import com.google.cloud.bigquery.BigQuery
import com.google.cloud.bigquery.BigQueryOptions
import com.google.cloud.bigquery.DatasetInfo
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient
import com.google.cloud.bigquery.storage.v1.BigQueryWriteSettings


/**
 * Get a reference on the BigQuery API for the given project.
 * Uses Google default credentials.
 */
fun bigQuery(projectId: String): BigQuery =
    BigQueryOptions.newBuilder()
        .setCredentials(defaultCredentials)
        .setProjectId(projectId)
        .build()
        .service


/**
 * Build the client to use the BigQuery Storage API, using default Credentials
 */
fun BigQueryWriteClient(): BigQueryWriteClient = BigQueryWriteClient.create(
    BigQueryWriteSettings.newBuilder()
        .setCredentialsProvider(com.google.api.gax.core.FixedCredentialsProvider.create(defaultCredentials))
        .build()
)


/**
 * Creates a dataset if it doesn't exist.
 * Use 'EU' as default location.
 */
fun BigQuery.createDatasetIfNeeded(datasetId: String, location: String = "EU") {
    val ds = getDataset(datasetId)
    if (ds == null) {
        val datasetInfo = DatasetInfo
            .newBuilder(datasetId)
            .setLocation(location)
            .build()
        create(datasetInfo, BigQuery.DatasetOption.fields())
    }
}

