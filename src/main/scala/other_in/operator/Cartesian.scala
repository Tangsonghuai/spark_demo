package other_in.operator


import org.apache.spark.{SparkConf, SparkContext}
import other_in.utils.SparkUtils

/** *****************************************************************************
  *
  * @date 2019-08-07 17:04
  * @author: <a href=mailto:huangyr>黄跃然</a>
  * @Description: 两个RDD进行笛卡尔积合并--The two RDD are Cartesian product merging
  ******************************************************************************/
object Cartesian {


  def cartesian(sparkContext: SparkContext): Unit = {
    val names = List("张三", "李四", "王五")
    val scores = List(60, 70, 90)

    val namesRDD = sparkContext.parallelize(names)
    val scoresRDD = sparkContext.parallelize(scores)

    val cartesianRDD = namesRDD.cartesian(scoresRDD)

    cartesianRDD.foreach(tuple => {
      println("key:"+tuple._1+"\tvalue:"+tuple._2)
    })

  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Ex2_Computations").setMaster("local[4]")
    val sparkContext = new SparkContext(conf)

    cartesian(sparkContext)
  }

}
