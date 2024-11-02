import org.apache.spark.{SparkConf, SparkContext}
import com.mongodb.spark._
import com.mongodb.spark.config._
import org.bson.Document
import scala.collection.JavaConverters._

object SavedToDatabase {
  def main(args: Array[String]): Unit = {
    println("Hey!")
    val conf = new SparkConf()
      .setAppName("Save Words to MongoDB")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    val textFileRDD = sc.textFile("data/wholeInvertedIndex/part-00000")

    val documentsRDD = textFileRDD.map { line =>
      val parts = line.split(", ")
      val word = parts(0)
      val docCount = parts(1).toInt
      val docIds = parts.drop(2).toSeq.asJava // to get all elements except the first two elements
      new Document("word", word)
        .append("docCount", docCount)
        .append("docIds", docIds)
    }
    documentsRDD.collect().take(10).foreach(println)
    documentsRDD.saveToMongoDB(WriteConfig(Map("uri" -> "mongodb://localhost:27017/BigData.dictionary")))

  }
}