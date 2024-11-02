import org.apache.spark.{SparkConf, SparkContext}
import com.mongodb.spark._
import com.mongodb.spark.config._
import scala.collection.JavaConverters._
import org.apache.log4j.BasicConfigurator
import org.apache.log4j.varia.NullAppender

object SearchTask {
  def main(args: Array[String]): Unit = {

    val nullAppender = new NullAppender
    BasicConfigurator.configure(nullAppender)

    val conf = new SparkConf()
      .setAppName("MongoDB RDD Connector")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    //load collection

    val mongoUri = "mongodb://localhost:27017/BigData.dictionary"
    val DocRDD = MongoSpark.load(sc, ReadConfig(Map("uri" -> mongoUri)))

    println("Enter your query:")
    val userInput = scala.io.StdIn.readLine()
    //println(userInput)

    val processedQuery = userInput.toLowerCase.split("\\s+").toSeq
    println(processedQuery)


    // Search in collection

    /*
    this is just for one word in the query
    val filteredRDD = DocRDD.filter(doc => doc.getString("word") == userInput)
      .flatMap(doc => doc.getList("docIds", classOf[String]).asScala) // Extract docIds only
    */


    // Collect document IDs for each word
    val docIdRdds = processedQuery.map{ word => //a sequence of RDDs, where each RDD contains the document IDs for word .
      DocRDD.filter(doc => doc.getString("word") == word)
        .flatMap(doc => doc.getList("docIds", classOf[String]).asScala)
    }

    docIdRdds.zip(processedQuery).foreach { case (rdd, word) =>
      val docIds = rdd.collect()  //collect each rdd
      println(s"Document IDs for '$word': ${docIds.mkString(", ")}")}

    val intersectedDocIds = docIdRdds.reduce((rdd1, rdd2) => rdd1.intersection(rdd2)) //operation on two rdd

    println("The Documents that these words are mentioned in are:")
    intersectedDocIds.collect().foreach(println)


  }
}
