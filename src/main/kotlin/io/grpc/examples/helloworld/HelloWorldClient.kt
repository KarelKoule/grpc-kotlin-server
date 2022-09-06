/*
 * Copyright 2020 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.grpc.examples.helloworld

import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import io.grpc.StatusException
import io.grpc.examples.helloworld.GreeterGrpcKt.GreeterCoroutineStub
import kotlinx.coroutines.*
import mu.KotlinLogging
import java.io.Closeable
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

class HelloWorldClient(val channel: ManagedChannel) : Closeable {
    private val stub: GreeterCoroutineStub = GreeterCoroutineStub(channel)

    suspend fun greet(s: String, sleep: Long = 0) {
        val request = helloRequest { message = s; this.sleep = sleep }
        try {
            logger.info("Start greeting: ${request.message} and sleep: ${request.sleep}")
            val response = stub.withDeadlineAfter(2000, TimeUnit.MILLISECONDS).sayHello(request)
            logger.info("Greeter client received: ${response.message}")
        } catch (e: StatusException) {
            logger.info("RPC failed: ${request.message} and sleep: ${request.sleep}")
        }
    }

    override fun close() {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS)
    }
}

/**
 * Greeter, uses first argument as name to greet if present;
 * greets "world" otherwise.
 */
fun main(args: Array<String>) {
    val isRemote = args.size == 1

    Executors.newFixedThreadPool(10).asCoroutineDispatcher().use { dispatcher ->
        val builder = if (isRemote)
            ManagedChannelBuilder.forTarget(args[0].removePrefix("https://") + ":443").useTransportSecurity()
        else
            ManagedChannelBuilder.forTarget("localhost:50051").usePlaintext()

        HelloWorldClient(
            builder.executor(dispatcher.asExecutor()).build()
        ).use {
            runBlocking(Dispatchers.IO) {
                async {it.greet("very long", 6000)}
                async {it.greet("long", 1000)}
                async {it.greet("long", 1000)}
                async {it.greet("long", 1000)}
                async {it.greet("long", 1000)}
                async {it.greet("long", 1000)}
                async {it.greet("long", 1000)}
                async {it.greet("long", 1000)}
                async {it.greet("long", 1000)}
                async {it.greet("long", 1000)}
                async {it.greet("long", 1000)}
                async {it.greet("quick", 0)}
                async {it.greet("quick", 10)}
                async {it.greet("quick", 100)}
                async {it.greet("longer", 2000)}
            }
        }
    }
}
