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

import io.grpc.Context
import io.grpc.Server
import io.grpc.ServerBuilder
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.filterNotNull
import kotlinx.coroutines.flow.first
import mu.KotlinLogging
import java.util.concurrent.Executors

val logger = KotlinLogging.logger {}

class HelloWorldServer(val port: Int) {
    val server: Server = ServerBuilder
        .forPort(port)
        .addService(HelloWorldService())
        .build()

    fun start() {
        server.start()
        logger.info("Server started, listening on $port")
        Runtime.getRuntime().addShutdownHook(
            Thread {
                logger.info("*** shutting down gRPC server since JVM is shutting down")
                stop()
                logger.info("*** server shut down")
            }
        )
    }

    private fun stop() {
        server.shutdown()
    }

    fun blockUntilShutdown() {
        server.awaitTermination()
    }

    private class HelloWorldService : GreeterGrpcKt.GreeterCoroutineImplBase() {
        val sleeper = SleepingClient()
        val pool = Executors.newFixedThreadPool(20)
        override suspend fun sayHello(request: HelloRequest): HelloReply = coroutineScope {

            getData(request)
//
//            channelFlow {
//                launch { send(response.await()) }
//             }.filterNotNull().first()

        }

        suspend fun getData(request: HelloRequest): HelloReply = coroutineScope {
            pool.run {
                launch {
                    Thread.sleep(2000)
//                    delay(2000)
                    logger.info { "log after 2000ms" }
                }
            }


//            CancellableContext withCancellation = Context.current().withCancellation();
//            try {
//                withCancellation.run(() -> {
//                    stub.streamFooUpdates(...);
//                    Thread.sleep(10000);
//                    withCancellation.cancel(null);
//                });
//            } finally {
//                withCancellation.cancel(null);
//            }


            logger.info { "Deadline is set to: ${Context.current().deadline}" }

            logger.info { "start responding with ${request.message} and timeout: ${request.sleep}" }
//                call.respond(HttpStatusCode.BadRequest, ErrorResponse("reason of failure"))
//                call.respond(HttpStatusCode.BadRequest, "text error")

            val clientData = async {
                val data = sleeper.getData(request.sleep)
                logger.info { "obtained data: '$data' with timeout ${request.sleep}" }

//                channelFlow {
//                        launch {send(data)}
//                    launch { send(sleeper.getData(500)) }
//                }

                data
            }

            val defaultData = async {
                delay(600)
                "default value for ${request.sleep}"
            }

            val result = channelFlow {
                launch { send(clientData.await()) }
                launch { send(defaultData.await()) }
            }.filterNotNull().first()

            logger.info { "finished responding ${request.message} with $result adn timeout ${request.sleep}" }

            helloReply { message = result }
        }
    }
}

fun main() {
    val port = System.getenv("PORT")?.toInt() ?: 50051
    val server = HelloWorldServer(port)
    server.start()
    server.blockUntilShutdown()
}
