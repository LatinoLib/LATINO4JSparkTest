import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.feature.IDF

import scala.collection.JavaConversions._

import org.latinolib.tokenizer.{Tokenizer, RegexTokenizers}
import org.latinolib.Language._
import org.latinolib.stemmer.Stemmer

object Latino4SparkTest
{
  def dropHeader(idx: Int, iter: Iterator[String]) = {
    if (idx == 0) { iter.drop(1) }
    iter
  }

  def preprocess(text: String): String =
    text
      .split('\t')(2) // parse
      .replaceAll("""(?i)http\S*""", "") // remove URLs
      .replaceAll("""(?i)\$\w+""", "") // remove stock refs
      .replaceAll("""(?i)@\w+""", "") // remove user refs
      .replaceAll("""(?i)(.)\1{2,}""", "$1$1") // handle letter repetitions
      .replaceAll("'", "") // remove apostrophes
      .replaceAll("""(?i)^rt\w""", "") // remove RTs
      .toLowerCase

  def tokenize(text: String)(implicit tokenizer: Tokenizer) =
    tokenizer.getTokens(text).map(_.getText)

  def lemmatize(words: Iterable[String])(implicit lemmatizer: Stemmer) =
    words.map(lemmatizer.getStem)

  def includeBigrams(words: Iterable[String]) =
    (words ++ words.sliding(2).map(item => item.head + " " + item.last)).toSeq

  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "c:\\hadoop\\")

    val conf = new SparkConf().setAppName("LATINO4J Sentiment Classification Test").setMaster("local")
    val sc = new SparkContext(conf)

    implicit val tokenizer = sc.broadcast(RegexTokenizers.LATIN.get).value
    implicit val lemmatizer = sc.broadcast(EN.getLemmatizer).value

    val root = "C:\\Work\\Scala\\Latino4SparkTest\\data"

    (0 to 9)
      .map(i => (s"$root\\fold_${i}_train", s"$root\\fold_${i}_test"))
      .foreach(foldFileNames => {

        val docs = sc.textFile(foldFileNames._1) // read one fold of training data
          .mapPartitionsWithIndex(dropHeader)
          .map(preprocess)
          .map(tokenize)
          .map(lemmatize)
          .map(includeBigrams)

        val tfs = new HashingTF().transform(docs)
        tfs.cache

        val tfidfs = new IDF(minDocFreq = 5).fit(tfs).transform(tfs)

        tfidfs.collect
          .take(10)
          .foreach(tweet => {
            println("====================================================================")
            println("====================================================================")
            println("====================================================================")
            println(tweet)
          })
      })
  }
}
