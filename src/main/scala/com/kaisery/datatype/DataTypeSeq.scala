package com.kaisery.datatype

import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalTime}

import com.kaisery.common.DataType

import scala.io.Source

case class DataWithLabel(label: Double, content: String)

object DataTypeSeq {

    val sampleCount = 10000
    val dateFormatter = new SimpleDateFormat("hh:mm:ss")

    def dataTypeSeqMap(): collection.mutable.Map[String, Seq[DataWithLabel]] = {
        collection.mutable.Map[String, Seq[DataWithLabel]](
            "integer #01" -> integerSeq(),
            "integer #02" -> integerSeq_1(),
            "integer #03" -> integerSeq_2(),
            "double #01" -> doubleSeq(),
            "double #02" -> doubleSeq_1(),
            "double #03" -> doubleSeq_2(),
            "double #04" -> doubleSeq_3(),
            "randomUnicodeText #01" -> randomUnicodeTextSeq(),
            "randomAlphanumericText #01" -> randomAlphanumericTextSeq(),
            "randomAlphabetText #01" -> randomAlphabetTextSeq(),
            "email #01" -> emailSeq(),
            "postcode #01" -> postcodeSeq(),
            "randomFakeEmail #01" -> randomFakeEmailSeq(),
            "randomFakeEmail #02" -> randomFakeEmailSeq_1(),
            "randomFakeEmail #03" -> randomFakeEmailSeq_2(),
            "randomFakeEmail #04" -> randomFakeEmailSeq_3(),
            "randomFakeEmail #05" -> randomFakeEmailSeq_4(),
            "ipv4 #02" -> ipv4Seq(),
            "randomFakeIPv4 #01" -> randomFakeIPv4Seq(),
            "randomFakeIPv4 #02" -> randomFakeIPv4Seq_1(),
            "uuid #01" -> uuidSeq(),
            "date #01" -> dateSeq(),
            "date #02" -> dateSeq_1(),
            "time #01" -> timeSeq(),
            "time #02" -> timeSeq_1()
        )
    }

    def integerSeq(): Seq[DataWithLabel] = {
        Seq.fill(sampleCount) {
            val label = DataType.INTEGER.id.toDouble
            val content = scala.util.Random.nextInt().toString
            DataWithLabel(label, content)
        }
    }

    def integerSeq_1(): Seq[DataWithLabel] = {
        Seq.tabulate(sampleCount)(n => {
            val label = DataType.INTEGER.id.toDouble
            val content = n.toString
            DataWithLabel(label, content)
        })
    }

    def integerSeq_2(): Seq[DataWithLabel] = {
        Seq.tabulate(sampleCount)(n => {
            val label = DataType.INTEGER.id.toDouble
            val content = ((n + 1) * -1).toString
            DataWithLabel(label, content)
        })
    }

    def doubleSeq(): Seq[DataWithLabel] = {
        Seq.fill(sampleCount) {
            val label = DataType.DOUBLE.id.toDouble
            val content = (scala.util.Random.nextDouble() * scala.util.Random.nextInt() * 100000000d).toString
            DataWithLabel(label, content)
        }
    }

    def doubleSeq_1(): Seq[DataWithLabel] = {
        Seq.fill(sampleCount) {
            val label = DataType.DOUBLE.id.toDouble
            val content = (scala.util.Random.nextDouble() * scala.util.Random.nextInt() * -100000000d).toString
            DataWithLabel(label, content)
        }
    }

    def doubleSeq_2(): Seq[DataWithLabel] = {
        Seq.fill(sampleCount) {
            val label = DataType.DOUBLE.id.toDouble
            val content = (scala.util.Random.nextDouble() + scala.util.Random.nextInt()).toString
            DataWithLabel(label, content)
        }
    }

    def doubleSeq_3(): Seq[DataWithLabel] = {
        Seq.fill(sampleCount) {
            val label = DataType.DOUBLE.id.toDouble
            val content = ((scala.util.Random.nextDouble() + scala.util.Random.nextInt()) * -1.0).toString
            DataWithLabel(label, content)
        }
    }

    def randomUnicodeTextSeq(): Seq[DataWithLabel] = {
        Seq.fill(sampleCount) {
            val label = DataType.TEXT.id.toDouble
            val content = scala.util.Random.nextString(scala.util.Random.nextInt(20) + 1).toString
            DataWithLabel(label, content)
        }
    }

    def randomAlphanumericTextSeq(): Seq[DataWithLabel] = {
        Seq.fill(sampleCount) {
            val label = DataType.TEXT.id.toDouble
            val content = scala.util.Random.alphanumeric.take(scala.util.Random.nextInt(20) + 1).mkString
            DataWithLabel(label, content)
        }
    }

    def randomAlphabetTextSeq(): Seq[DataWithLabel] = {
        Seq.fill(sampleCount) {
            val label = DataType.TEXT.id.toDouble
            val content = scala.util.Random.alphanumeric.take(20).filter(c => c >= 'A' && c <= 'z').mkString
            DataWithLabel(label, content)
        }
    }

    def emailSeq(): Seq[DataWithLabel] = {
        Source.fromInputStream(this.getClass.getResourceAsStream("/email.csv")).getLines().map(line => {
            val label = DataType.EMAIL.id.toDouble
            val content = line
            DataWithLabel(label, content)
        }).toSeq
    }

    def postcodeSeq(): Seq[DataWithLabel] = {
        Source.fromInputStream(this.getClass.getResourceAsStream("/postcode.csv")).getLines().map(line => {
            val label = DataType.POST_CODE.id.toDouble
            val content = line
            DataWithLabel(label, content)
        }).toSeq
    }

    def randomFakeEmailSeq(): Seq[DataWithLabel] = {
        Seq.fill(sampleCount) {
            val label = DataType.FAKE_EMAIL.id.toDouble
            val content = scala.util.Random.alphanumeric.take(scala.util.Random.nextInt(10) + 1).mkString + "@" +
                scala.util.Random.alphanumeric.take(scala.util.Random.nextInt(5) + 1)
            DataWithLabel(label, content)
        }
    }

    def randomFakeEmailSeq_1(): Seq[DataWithLabel] = {
        Seq.fill(sampleCount) {
            val label = DataType.FAKE_EMAIL.id.toDouble
            val content = scala.util.Random.alphanumeric.take(scala.util.Random.nextInt(10) + 1).mkString + "@" +
                scala.util.Random.alphanumeric.take(scala.util.Random.nextInt(5) + 1) + "." +
                scala.util.Random.alphanumeric.take(scala.util.Random.nextInt(3) + 1).mkString
            DataWithLabel(label, content)
        }
    }

    def randomFakeEmailSeq_2(): Seq[DataWithLabel] = {
        Seq.fill(sampleCount) {
            val label = DataType.FAKE_EMAIL.id.toDouble
            val content = scala.util.Random.alphanumeric.take(scala.util.Random.nextInt(3) + 1).mkString + "@" +
                scala.util.Random.alphanumeric.take(scala.util.Random.nextInt(3) + 1) + "@" +
                scala.util.Random.alphanumeric.take(scala.util.Random.nextInt(3) + 1).mkString + "." +
                scala.util.Random.alphanumeric.take(scala.util.Random.nextInt(3) + 1).mkString
            DataWithLabel(label, content)
        }
    }

    def randomFakeEmailSeq_3(): Seq[DataWithLabel] = {
        Seq.fill(sampleCount) {
            val label = DataType.FAKE_EMAIL.id.toDouble
            val content = scala.util.Random.alphanumeric.take(scala.util.Random.nextInt(3) + 1).mkString + "@" +
                scala.util.Random.alphanumeric.take(scala.util.Random.nextInt(3) + 1) + "@" +
                scala.util.Random.alphanumeric.take(scala.util.Random.nextInt(3) + 1).mkString + "." +
                scala.util.Random.alphanumeric.take(scala.util.Random.nextInt(3) + 1).mkString + "." +
                scala.util.Random.alphanumeric.take(scala.util.Random.nextInt(3) + 1).mkString
            DataWithLabel(label, content)
        }
    }

    def randomFakeEmailSeq_4(): Seq[DataWithLabel] = {
        Seq.fill(sampleCount) {
            val label = DataType.FAKE_EMAIL.id.toDouble
            val content = scala.util.Random.nextInt(1000) + "@" + scala.util.Random.nextInt(1000)
            DataWithLabel(label, content)
        }
    }

    def ipv4Seq(): Seq[DataWithLabel] = {
        Source.fromInputStream(this.getClass.getResourceAsStream("/ipv4.csv")).getLines().map(line => {
            val label = DataType.IP_V4.id.toDouble
            val content = line
            DataWithLabel(label, content)
        }).toSeq
    }

    def randomFakeIPv4Seq(): Seq[DataWithLabel] = {
        Seq.fill(sampleCount) {
            val label = DataType.FAKE_IP_V4.id.toDouble
            val content = (scala.util.Random.nextInt(744) + 256) + "." +
                (scala.util.Random.nextInt(744) + 256) + "." +
                (scala.util.Random.nextInt(744) + 256) + "." +
                (scala.util.Random.nextInt(744) + 256)
            DataWithLabel(label, content)
        }
    }

    def randomFakeIPv4Seq_1(): Seq[DataWithLabel] = {
        Seq.fill(sampleCount) {
            val label = DataType.FAKE_IP_V4.id.toDouble
            val content = scala.util.Random.nextInt(1000) + "." +
                scala.util.Random.nextInt(1000) + "." +
                scala.util.Random.nextInt(1000)
            DataWithLabel(label, content)
        }
    }

    def uuidSeq(): Seq[DataWithLabel] = {
        Source.fromInputStream(this.getClass.getResourceAsStream("/uuid.csv")).getLines().map(line => {
            val label = DataType.UUID.id.toDouble
            val content = line
            DataWithLabel(label, content)
        }).toSeq
    }

    def dateSeq(): Seq[DataWithLabel] = {
        val dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
        Seq.fill(sampleCount) {
            val label = DataType.DATE.id.toDouble
            val content = LocalDate.of(
                scala.util.Random.nextInt(9999) + 1,
                scala.util.Random.nextInt(12) + 1,
                scala.util.Random.nextInt(28) + 1
            ).format(dateTimeFormatter)
            DataWithLabel(label, content)
        }
    }

    def dateSeq_1(): Seq[DataWithLabel] = {
        val dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy年MM月dd日")
        Seq.fill(sampleCount) {
            val label = DataType.DATE.id.toDouble
            val content = LocalDate.of(
                scala.util.Random.nextInt(9999) + 1,
                scala.util.Random.nextInt(12) + 1,
                scala.util.Random.nextInt(28) + 1
            ).format(dateTimeFormatter)
            DataWithLabel(label, content)
        }
    }

    def timeSeq(): Seq[DataWithLabel] = {
        val dateTimeFormatter = DateTimeFormatter.ofPattern("hh:mm:ss")
        Seq.fill(sampleCount) {
            val label = DataType.TIME.id.toDouble
            val content = LocalTime.of(
                scala.util.Random.nextInt(23) + 1,
                scala.util.Random.nextInt(59) + 1,
                scala.util.Random.nextInt(59) + 1
            ).format(dateTimeFormatter)
            DataWithLabel(label, content)
        }
    }

    def timeSeq_1(): Seq[DataWithLabel] = {
        val dateTimeFormatter = DateTimeFormatter.ofPattern("hh时mm分ss秒")
        Seq.fill(sampleCount) {
            val label = DataType.TIME.id.toDouble
            val content = LocalTime.of(
                scala.util.Random.nextInt(23) + 1,
                scala.util.Random.nextInt(59) + 1,
                scala.util.Random.nextInt(59) + 1
            ).format(dateTimeFormatter)
            DataWithLabel(label, content)
        }
    }
}
