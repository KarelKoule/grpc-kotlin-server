package io.grpc.examples.helloworld

import kotlinx.coroutines.delay

class SleepingClient {
    suspend fun getData(howLongToSleep: Long): String {
        delay(howLongToSleep)

        return "sleeped for $howLongToSleep"
    }
}
