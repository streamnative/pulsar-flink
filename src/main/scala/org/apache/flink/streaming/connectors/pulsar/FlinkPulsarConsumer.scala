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

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.runtime.state.{CheckpointListener, FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.table.dataformat.BinaryRow

class FlinkPulsarConsumer
    extends RichParallelSourceFunction[BinaryRow]
    with ResultTypeQueryable[BinaryRow]
    with CheckpointListener
    with CheckpointedFunction {

  override def getProducedType: TypeInformation[BinaryRow] = ???

  override def notifyCheckpointComplete(l: Long): Unit = ???

  override def snapshotState(
    functionSnapshotContext: FunctionSnapshotContext): Unit = ???

  override def initializeState(
    functionInitializationContext: FunctionInitializationContext): Unit = ???

  override def run(
    sourceContext: SourceFunction.SourceContext[BinaryRow]): Unit = ???

  override def cancel(): Unit = ???
}
