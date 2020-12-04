/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.pulsar.source.util;

import org.apache.flink.util.function.BiConsumerWithException;
import org.apache.flink.util.function.FunctionWithException;

import org.apache.pulsar.client.admin.PulsarAdmin;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiPredicate;

/**
 * A util class for asynchronous call of method.
 */
public class AsyncUtils {
    public static <T, R, C extends CompletableFuture<R>, E extends Exception> void parallelAsync(
            List<T> elements,
            FunctionWithException<T, C, E> asyncInvoker,
            BiConsumerWithException<T, R, E> consumer,
            Class<E> exceptionClass) throws E, InterruptedException, TimeoutException {
        parallelAsync(elements, asyncInvoker, (topic, exception) -> false, consumer, exceptionClass);
    }

    public static <T, R, C extends CompletableFuture<R>, E extends Exception> void parallelAsync(
            List<T> elements,
            FunctionWithException<T, C, E> asyncInvoker,
            BiPredicate<T, E> swallowException,
            BiConsumerWithException<T, R, E> consumer,
            Class<E> exceptionClass) throws E, InterruptedException, TimeoutException {
        List<C> asyncFutures = new ArrayList<>();
        for (T element : elements) {
            asyncFutures.add(asyncInvoker.apply(element));
        }

        for (int index = 0; index < asyncFutures.size(); index++) {
            try {
                R result = asyncFutures.get(index).get(PulsarAdmin.DEFAULT_READ_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                consumer.accept(elements.get(index), result);
            } catch (ExecutionException e) {
                E cause = exceptionClass.cast(e.getCause());
                if (!swallowException.test(elements.get(index), cause)) {
                    throw cause;
                }
            } catch (TimeoutException e) {
                throw new TimeoutException("Timeout while waiting for " + elements.get(index));
            }
        }
    }
}
