package Jobs

import org.apache.spark.sql.SparkSession

//import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.functions._
// Object est un peu l'equivalent du public main void java
object Test extends App {
  val Job = "Job_name"

  val spark = SparkSession
    .builder()
    .appName(Job)
    .enableHiveSupport()
    .getOrCreate()
  import spark.implicits._
  val dataSeq = Seq(("Java", 20000), ("Python", 100000), ("Scala", 3000))
  val rdd=spark.sparkContext.parallelize(dataSeq)
  
  val data=rdd.toDF("Matiere","total")
  data.show
}
