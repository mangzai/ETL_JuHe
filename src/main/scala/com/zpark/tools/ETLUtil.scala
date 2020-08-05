package com.zpark.tools

import java.util.Properties

import com.zpark.juhe.ETLXinWen.data
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}



/**
 * @author ys
 *         data 2020/7/27 14:05
 */
object ETLUtil {

  val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("ETL")
  val sparkSession: SparkSession = SparkSession.builder().config(conf).getOrCreate()
  val sqlContext: SQLContext = sparkSession.sqlContext




  def savedata1(table: String, sql: String): Unit = {


    val (url: String, props: Properties) = resouces
    import sparkSession.implicits._
    val juhe: RDD[data] = toCount(sql).map(a => {
      data(a._1, a._2)
    })
    val df: DataFrame = juhe.toDF()


    df.write.mode("append").jdbc(url, table, props)
    df.show()
  }


  def resouces = {
    val url = "jdbc:mysql://localhost:3306/db_juhe?useUnicode=true&characterEncoding=UTF-8"
    val props = new Properties()
    props.put("user", "root")
    props.put("password", "rootroot")
    props.put("driver", "com.mysql.jdbc.Driver")
    (url, props)
  }

  /*关闭流
    * */
  def close: Unit = {
    sparkSession.close()
  }

  /*统计category的个数

  * */

  def toCount(sql: String): RDD[(String, Int)] = {
    val frame: DataFrame = sqlContext.sql(sql)
    val rdd: RDD[String] = frame.rdd.map(x => x.mkString(","))

    val word: RDD[String] = rdd.flatMap(line => line.split(","))
    //val word: RDD[String] = rdd.flatMap(line => line.split(",")).filter(x => x.eq("(") | x.eq(")"))
    val count: RDD[(String, Int)] = word.filter(x => !x.contains("("))
      .filter(x => !x.contains(")"))
      .map(x => (x, 1)).reduceByKey((x, y) => x + y)
    count
  }


}
