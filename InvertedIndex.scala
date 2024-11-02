import org.apache.spark.{SparkConf, SparkContext}

object InvertedIndex {
  def main(args: Array[String]): Unit = {
    println("Hey!")

    val conf = new SparkConf()
      .setAppName("Inverted Index Task")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)
    
    val DocsRDD  = sc.wholeTextFiles("data/BigTemp", 8)

    // to get just the doc_id not whole path
    val DocRddWithDocId = DocsRDD.map{case(filepath, content) => (filepath.split('/').last, content)}

    // (Tokenization)
    // Split the doc content based on space /done
    // Remove stop words , OR any symbols , Or numbers /done
    // make all words in Lowercase /done
    // Get the unique words from each content /done
    // at the end we should have (doc_id , [words]) /done
    // each word from each array will be as a key and (doc_id,1) as value /done
    // Reduced by key and sum up the docs so w1, (5 , [do1 , do2 , doc3]) /done
    // ensure that all docs are ordered /done
    // save the result in the txt. file /done

    val stopWords = Set("the", "is", "in", "and", "to", "of", "a", "for", "on", "with", "as", "it", "that", "by", "this" , "at" ,"its")
    val regexSymbols = "[^a-z]+".r // to Remove all symbols , digits from words .

    val splittedDocRddWithDocId = DocRddWithDocId
      .mapValues(content => content.split("\\s+") // one or more whitespace
        .map(word => word.toLowerCase()))

    //Remove single char and all symbols

    val cleanDocRddWithDocId = splittedDocRddWithDocId
      .mapValues(content => content.map(word => word.replaceAll(regexSymbols.regex, "")).filterNot(word => word.length == 1).filter(_.nonEmpty))

    //To Remove all stop words
    val toknizeDocRddWithDocId = cleanDocRddWithDocId.mapValues(content => content.filterNot(word => stopWords.contains(word)))

    //get unique words for each content .

    val distinctwordRDD = toknizeDocRddWithDocId.mapValues(content => content.distinct)

    // to convert it to become like (w1 , (doc1 ,1))

    val keyWordRDD = distinctwordRDD.flatMap{case(doc_id , content) => content.map(word => (word , (List(doc_id) ,1)))}
    //I used flatMap to return tuple not array

    val finalRDD = keyWordRDD
      .reduceByKey{case ((doc1, count1), (doc2, count2)) =>
       val combinedList = (doc1 ++ doc2).sorted
        (combinedList, count1 + count2)}.sortByKey()

    val formattedRDD = finalRDD.map {
      case (word, (docIds, totalCount)) =>
        s"$word, $totalCount, ${docIds.mkString(", ")}"
    }
    //save the result to txt file
    formattedRDD.coalesce(1).saveAsTextFile("data/wholeInvertedIndex")
    //use .coalesce(1) to save them in just one part

    distinctwordRDD.collect().take(10).foreach{case(doc_id, wordsArray) =>
      val arrayString = wordsArray.mkString(" ")
      println(s"Doc ID: $doc_id , Words: [$arrayString]")}

    keyWordRDD.collect().take(200).foreach {
      case (word, (docId, count)) => println(word,(docId,count))
    }


    //print the final Result
    finalRDD.collect().foreach {
      case (word, (docIds, totalCount)) => println(s"$word: (${docIds.mkString(", ")}, $totalCount)")
    }


    sc.stop()
  }
}

