package sql

import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

//
// Define data in terms of a case class, convert it to a DataFrame,
// register it as a temporary table, and query it. Print the original DataFrame
// and the ones resulting from the queries, and see their schema.
//
object Basic {
  case class Cust(id: Integer, name: String, sales: Double, discount: Double, state: String)

  def main(args: Array[String]) {
    val spark =
      SparkSession.builder()
        .appName("SQL-Basic")
//        .master("spark://192.168.2.120:7077")
      .master("local[2]")
        .getOrCreate()

    import spark.implicits._

    // create a sequence of case class objects
    // (we defined the case class above)
    val custs = Seq(
      Cust(5, "Ye Olde Widgete", 500.00, 0.0, "MA"),
      Cust(1, "Widget Co", 120000.00, 0.00, "AZ"),
      Cust(1, "Widget Co", 120010.00, 7.00, "AZ"),
      Cust(1, "Widget Co", 120010.00, 2.00, "AZ"),
      Cust(1, "Widget Co", 120010.00, 3.00, "AZ"),
      Cust(1, "Widget Co", 120050.00, 1.00, "AZ"),
      Cust(1, "Widget ", 120430.00, 56.00, "AZ"),
      Cust(1, "Widget Go", 120430.00, 56.00, "AZ"),
      Cust(1, "Widget ", 120430.00, 56.00, "AZ"),
      Cust(2, "Acme Widgets", 410500.00, 500.00, "CA"),
      Cust(3, "Widgetry", 410500.00, 200.00, "CA"),
      Cust(4, "Widgets R Us", 410500.00, 0.0, "CA"),
      Cust(5, "Ye Olde Widgete", 500.00, 0.0, "MA")
    )
    // make it an RDD and convert to a DataFrame
    val customerDF = spark.sparkContext.parallelize(custs, 4).toDF()

//    println("*** See the DataFrame contents")
//    customerDF.show()
//
//    println("*** See the first few lines of the DataFrame contents")
//    customerDF.show(2)
//
//    println("*** Statistics for the numerical columns")
//    customerDF.describe("sales", "discount").show()
//
//    println("*** A DataFrame has a schema")
//    customerDF.printSchema()

    //
    // Register with a table name for SQL queries
    //
    customerDF.createOrReplaceTempView("customer")

    println("*** Very simple query")
//    val allCust = spark.sql("SELECT id, name, sales , discount ,state," +
//      "row_number() over (order by sales desc  ) rn" +
//      " FROM customer")         // 单次排序    -->  sales


        val allCust = spark.sql("SELECT id, name, sales , discount ,state," +
          "row_number() over (partition by id,name   order by sales desc ,discount desc  ) rn" +
          " FROM customer")         // 分组排序 id 为组 ， 每一组按 sales 倒序排列
   println("LEEE")

    allCust.show()

    import org.apache.spark.sql.SaveMode

    val properties=new java.util.Properties()
    properties.setProperty("user","root")
    properties.setProperty("password","123456")

    allCust.write.mode(SaveMode.Overwrite).jdbc("jdbc:mysql://192.168.2.120:3306/test?useUnicode=true&characterEncoding=utf8&rewriteBatchedStatements=true&useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true","New_table_Really",properties)


    println("*** The result has a schema too")
    allCust.printSchema()

    //
    // more complex query: note how it's spread across multiple lines
    //
//    println("*** Very simple query with a filter")
//    val californiaCust =
//      spark.sql(
//        s"""
//          | SELECT id, name, sales
//          | FROM customer
//          | WHERE state = 'CA'
//         """.stripMargin)
//    californiaCust.show()
//    californiaCust.printSchema()
//
//    println("*** Queries are case sensitive by default, but this can be disabled")

    spark.conf.set("spark.sql.caseSensitive", "false")
    //
    // the capitalization of "CUSTOMER" here would normally make the query fail
    // with "Table not found"
    //
//    val caseInsensitive =
//      spark.sql("SELECT * FROM CUSTOMER")
//    caseInsensitive.show()
//    spark.conf.set("spark.sql.caseSensitive", "true")


  }
}
