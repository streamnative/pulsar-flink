/**
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
package org.apache.flink.streaming.connectors.pulsar

import java.util.concurrent.atomic.AtomicReference

/**
  * Creates an exception proxy that interrupts the given thread upon
  * report of an exception. The thread to interrupt may be null.
  *
  * @param toInterrupt The thread to interrupt upon an exception. May be null.
  */
class ExceptionProxy(toInterrupt: Thread) {

  /** The exception to throw. */
  private val exception: AtomicReference[Throwable] = new AtomicReference[Throwable]

  /**
    * Sets the exception and interrupts the target thread,
    * if no other exception has occurred so far.
    *
    * <p>The exception is only set (and the interruption is only triggered),
    * if no other exception was set before.
    *
    * @param t The exception that occurred
    */
  def reportError(t: Throwable): Unit = {
    // set the exception, if it is the first (and the exception is non null)
    if (t != null && exception.compareAndSet(null, t) && toInterrupt != null) {
      toInterrupt.interrupt()
    }
  }

  /**
    * Checks whether an exception has been set via {@link #reportError(Throwable)}.
    * If yes, that exception if re-thrown by this method.
    *
    * @throws Exception This method re-throws the exception, if set.
    */
  @throws[Exception]
  def checkAndThrowException(): Unit = {
    val e = exception.get()
    if (e != null) {
      e match {
        case except: Exception => throw except
        case error: Error => throw error
        case _ => throw new Exception(e)
      }
    }
  }
}
