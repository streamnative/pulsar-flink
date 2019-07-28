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
package org.apache.flink.pulsar

import java.sql.Timestamp
import java.util
import java.util.Calendar

import scala.beans.BeanProperty
import scala.collection.JavaConverters._

object SchemaData {

  val booleanSeq = Seq(true, false, true, true, false)
  val bytesSeq = 1.to(5).map(_.toString.getBytes)

  val cal = Calendar.getInstance()
  cal.clear()
  val dateSeq = (1 to 5).map { i =>
    cal.set(2019, 0, i)
    cal.getTime
  }

  cal.clear()
  val timestampSeq = (1 to 5).map { i =>
    cal.set(2019, 0, i, 20, 35, 40)
    new Timestamp(cal.getTimeInMillis)
  }

  val stringSeq = 1.to(5).map(_.toString)
  val int8Seq = 1.to(5).map(_.toByte)
  val doubleSeq = 1.to(5).map(_.toDouble)
  val floatSeq = 1.to(5).map(_.toFloat)
  val int32Seq = 1.to(5)
  val int64Seq = 1.to(5).map(_.toLong)
  val int16Seq = 1.to(5).map(_.toShort)

  case class Foo(@BeanProperty i: Int, @BeanProperty f: Float, @BeanProperty bar: Bar)
  case class Bar(@BeanProperty b: Boolean, @BeanProperty s: String)

  case class F1(@BeanProperty baz: Baz)

  case class Baz(
      @BeanProperty f: Float,
      @BeanProperty d: Double,
      @BeanProperty mp: util.Map[String, Bar],
      @BeanProperty arr: Array[Bar])

  val fooSeq: Seq[Foo] =
    Foo(1, 1.0.toFloat, Bar(true, "a")) :: Foo(2, 2.0.toFloat, Bar(false, "b")) :: Foo(3, 0, null) :: Nil

  val f1Seq: Seq[F1] =
    F1(
      Baz(
        Float.NaN,
        Double.NaN,
        Map("1" -> Bar(true, "1"), "2" -> Bar(false, "2")).asJava,
        Array(Bar(true, "1"), Bar(true, "2")))) ::
    F1(
      Baz(
        Float.NegativeInfinity,
        Double.NegativeInfinity,
        Map("" -> Bar(true, "1")).asJava,
        null)) ::
    F1(Baz(Float.PositiveInfinity, Double.PositiveInfinity, null, null)) ::
    F1(Baz(1.0.toFloat, 2.0, null, null)) :: Nil

  val f1Results = f1Seq.map(f1 =>
    (f1.baz.f, f1.baz.d, if (f1.baz.mp == null) null else f1.baz.mp.asScala, f1.baz.arr))
}
