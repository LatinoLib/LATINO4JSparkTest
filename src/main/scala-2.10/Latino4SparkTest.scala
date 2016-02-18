import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import org.latinolib.Language

object Latino4SparkTest{
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "c:\\hadoop\\")

    val logFile = "C:\\spark\\README.md"
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile, 2).cache()
//    val numAs = logData.filter(line => line.contains("a")).count()
//    val numBs = logData.filter(line => line.contains("b")).count()
    val lang = logData
      .map(line => Language.detect(line))
      .groupBy(lang => lang)
      .map(item => (item._1, item._2.size))

    val langLocal = lang.collect()

    println("----------------------------------------------------------")
//    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
    langLocal.foreach(item => println(item))
    println("----------------------------------------------------------")
  }
}
