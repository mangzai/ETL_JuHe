package com.zpark.juhe

import com.zpark.tools.ETLUtil
import com.zpark.tools.ETLUtil.sqlContext
import org.apache.spark.sql.DataFrame

/**
 * @author ys
 *         data 2020/7/28 20:37
 */
object ETLWeather {
 // val dataFrame: DataFrame = sqlContext.read.format("json").load("in/weather.json")
 // dataFrame.createOrReplaceTempView("table")
 // dataFrame.createGlobalTempView("table")
 val dataFrame: DataFrame = sqlContext.read.format("json").load("in/weather.json")
  dataFrame.createOrReplaceTempView("table2")

  val sql2 = "select result.future.weather from table2"
  val sql3 = "select result.future.temperature from table2"
  val sql4="select result.future.direct from table2"
  def main(args: Array[String]): Unit = {
    println("++++++++++++++++")
    ETLUtil.toCount(sql2).foreach(println)
    ETLUtil.toCount(sql3).foreach(println)
    //ETLUtil.readdata(file)
    ETLUtil.savedata1("weather_weather", sql2)
    ETLUtil.savedata1("weather_wendu", sql3)
    ETLUtil.savedata1("weather_feng",sql4)
    ETLUtil.close
  }

  case class data(name: String, count: String)

}
