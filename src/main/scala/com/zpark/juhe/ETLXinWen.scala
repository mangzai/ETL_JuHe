package com.zpark.juhe


import com.zpark.tools.ETLUtil
import com.zpark.tools.ETLUtil.sqlContext
import org.apache.spark.sql.DataFrame

/**
 * @author ys
 *         data 2020/7/28 19:11
 */
object ETLXinWen  {
  /*sql*/
  val sql2 = "select result.data.category  from table1"

  val sql3 = "select result.data.author_name from table1"
  val sql4 = "select result.data.url from table1"

  val dataFrame: DataFrame = sqlContext.read.format("json").load("in/xinwen.json")

  dataFrame.createOrReplaceTempView("table1")
  def main(args: Array[String]): Unit = {

    println("++++++++++++++++")
    //  toCount(sql1).foreach(println)
    println("++++++++++++++++")
    ETLUtil.toCount(sql2).foreach(println)
    println("++++++++++++++++")
   ETLUtil. toCount(sql3).foreach(println)
    println("++++++++++++++++")
   ETLUtil. toCount(sql4).foreach(println)

   // ETLUtil.readdata(file)
    ETLUtil.savedata1( "xinwen_category",sql2)
    ETLUtil.savedata1("xinwen_author_name",sql3)
    ETLUtil.savedata1("xinwen_url",sql4)
    ETLUtil.close
  }
 // case class juhecategory(name: String, count: Int)

  //case class juheauthor_name(name: String, count: Int)



  case class data(name:String ,count:Int)
}
