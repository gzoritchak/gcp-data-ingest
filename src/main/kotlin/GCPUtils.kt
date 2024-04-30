package org.example


import com.google.auth.oauth2.GoogleCredentials
import java.util.logging.Level

typealias UtilLogger = java.util.logging.Logger

/**
 * The Google credentials using application default. It works the same way
 * on the developer machine and on the cloud.
 */
val defaultCredentials: GoogleCredentials by lazy { GoogleCredentials.getApplicationDefault() }


/**
 * Google libraries are a little chatty. Let's keep only warnings and error messages
 */
fun attenuateGoogleLogs() {
    UtilLogger.getLogger("com.google").level = Level.WARNING
}
