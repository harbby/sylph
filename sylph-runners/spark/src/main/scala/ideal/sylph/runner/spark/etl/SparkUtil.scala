package ideal.sylph.runner.spark.etl

import ideal.sylph.api.etl.RealTimeTransForm
import org.apache.spark.TaskContext
import org.apache.spark.sql.Row

object SparkUtil {
  val transFunction = (partition: Iterator[Row], realTimeTransForm: RealTimeTransForm) => {
    var errorOrNull: Exception = null
    val schema = realTimeTransForm.getRowSchema
    try {
      val partitionId = TaskContext.getPartitionId()
      if (realTimeTransForm.open(partitionId, 0))
        partition.flatMap(row => {
          //TODO: SparkRow.parserRow(x) with schema ?
          realTimeTransForm.process(SparkRow.make(row)).map(x => SparkRow.parserRow(x))
        })
      else Iterator.empty
    }
    catch {
      case e: Exception => errorOrNull = e
        Iterator.empty //转换失败 这批数据都丢弃
    } finally {
      realTimeTransForm.close(errorOrNull) //destroy()
    }
  }
}
