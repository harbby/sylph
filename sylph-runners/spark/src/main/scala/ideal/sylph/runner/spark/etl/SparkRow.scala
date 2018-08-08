package ideal.sylph.runner.spark.etl

import ideal.sylph.etl.Row.DefaultRow
import org.apache.spark.sql.Row

object SparkRow {
  def make(row: Row): SparkRow = new SparkRow(row)

  def parserRow(row: ideal.sylph.etl.Row): Row = row match {
    case row1: SparkRow => row1.get()
    case row1: DefaultRow => Row.apply(row1.getValues)
    case _ =>
      throw new RuntimeException(" not souch row type: " + row.getClass)
  }
}

class SparkRow(private val row: Row) extends ideal.sylph.etl.Row {

  def get() = row

  @Override
  override def mkString(seq: String): String = row.mkString(seq)

  override def getAs[T](key: String): T = row.getAs(key).asInstanceOf[T]

  override def getAs[T](i: Int): T = row.getAs(i).asInstanceOf[T]

  override def size(): Int = row.size

  override def toString(): String = row.toString()
}
