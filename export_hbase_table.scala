package io.github.zerix.utils.hbase

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by zery on 15-4-13.
 */

object export_hbase_table {

  class RDDMultipleTextOutputFormat extends MultipleTextOutputFormat[Any, Any] {
    override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String =
    key.asInstanceOf[String] + ".csv"
  }

  def main(args: Array[String]) {
    val Array(spark_master, zk_quorum, hbase_port, hbase_master, hbase_url, total_executor_num,
                            table_name, start_row, stop_row, output_file_path, task_type, group_field_index, prefix_field) = args

    /*add code to check argument valid*/

    val conf = new SparkConf()
      .setMaster(spark_master)
      .setAppName("export_hbase_data")
      .set("spark.akka.frameSize", "1024")
      .set("spark.kryoserializer.buffer.mb", "512")
      .set("spark.scheduler.mode", "FAIR")
      .set("spark.ui.port", "4042")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.rdd.compress", "true")
      .set("spark.io.compression.codec", "org.apache.spark.io.SnappyCompressionCodec")
      .set("spark.streaming.blockInterval", "1000")
      .set("spark.shuffle.manager", "SORT")
      .set("spark.eventLog.enabled", "true")
      .set("spark.scheduler.allocation.file", "./pool.xml")

    val sc = new SparkContext(conf)

    val export_db_conf = HBaseConfiguration.create()
    export_db_conf.set("hbase.zookeeper.property.clientPort", hbase_port)
    export_db_conf.set("hbase.zookeeper.quorum", hbase_master)
    export_db_conf.set(TableInputFormat.INPUT_TABLE, table_name)
    export_db_conf.set("hbase.master", hbase_url)
    export_db_conf.set(TableInputFormat.SCAN_ROW_START, start_row)
    export_db_conf.set(TableInputFormat.SCAN_ROW_STOP, stop_row)

    val export_db_rdd = sc.newAPIHadoopRDD(export_db_conf, classOf[TableInputFormat],
                          classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result]).map(_._2)

    val scan_hbase_table_data = export_db_rdd.map(rdded => {
      val rid = new String(rdded.getRow)
      val cells = rdded.listCells()

      val db_rowkey = rid
      var db_fields_line = prefix_field
      if (prefix_field.equals("rowkey")) {
        db_fields_line = rid
      }

      var grouped_field_value = "-"

      for (i <- 1 to cells.size()) {
        val field_value = new String(cells.get(i - 1).getValue)
        val field_name = new String(cells.get(i - 1).getQualifier)

        if (i == group_field_index) {
          grouped_field_value = field_value
        }
        db_fields_line += ("," + field_value)
      }

      (grouped_field_value, db_fields_line)
    })

    task_type match {
      case "1" => {
        /*add repartition() function for multi write thread?*/
        val export_hbase_week_data_task = scan_hbase_table_data.saveAsHadoopFile(output_file_path,
                                                                                classOf[String],
                                                                                classOf[String],
                                                                                classOf[RDDMultipleTextOutputFormat])
      }
      case "2" => {
        val export_hbase_month_data_task = scan_hbase_table_data.map(_._2)
                                                                .repartition(total_executor_num.toInt)
                                                                .saveAsTextFile(output_file_path)
      }
      case _ => {
        println("ERROR task_type argment, it must be 'week' or 'month'")
      }
    }

    sc.stop()
  }
}