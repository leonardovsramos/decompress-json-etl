import org.apache.spark.sql.api.java.UDF1

import java.io.ByteArrayInputStream
import java.util.zip.GZIPInputStream

class Decompress extends UDF1[Array[Byte], Array[Byte]] {
    override def call(t1: Array[Byte]): Array[Byte] = {
      val inputStream = new GZIPInputStream(new ByteArrayInputStream(t1))
      org.apache.commons.io.IOUtils.toByteArray(inputStream)
    }
}