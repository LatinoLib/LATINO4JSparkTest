import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import org.latinolib.Language

object Latino4SparkTest {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "c:\\hadoop\\")

    val conf = new SparkConf().setAppName("LATINO4J Hello World").setMaster("local")
    val sc = new SparkContext(conf)

    val logFile = "C:\\spark\\README.md"

    val logData = sc.textFile(logFile, 2).cache()
    val lang = logData
      .map(line => Language.detect(line))
      .groupBy(lang => lang)
      .map(item => (item._1, item._2.size))

    val langLocal = lang.collect()

    println("----------------------------------------------------------")
    langLocal.foreach(item => println(item))
    println("----------------------------------------------------------")
  }
}
