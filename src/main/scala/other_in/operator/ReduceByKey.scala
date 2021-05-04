package other_in.operator

import other_in.utils.Constant
import other_in.utils.SparkUtils
import org.apache.spark.{SparkConf, SparkContext}

/** *****************************************************************************
  * @date 2019-08-08 15:09
  * @author: <a href=mailto:huangyr>黄跃然</a>
  * @Description: 对<key, value>结构的RDD进行聚合，对具有相同key的value调用func来进行reduce操作，func的类型必须是(V, V) => V
  ******************************************************************************/
object ReduceByKey {

  def reduceByKey(sparkContext: SparkContext): Unit = {
    val rdd = sparkContext.textFile(Constant.LOCAL_FILE_PREX+"/data/resources/wc_data")
    rdd.flatMap(line => {
      line.split("\\s+")
    }).map((_, 1)).reduceByKey((pre, after) => {
      pre + after
    }).foreach(t => {
      println(t)
    })
  }


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Ex2_Computations").setMaster("local[4]")
    val sparkContext = new SparkContext(conf)

    reduceByKey(sparkContext)
  }

}
