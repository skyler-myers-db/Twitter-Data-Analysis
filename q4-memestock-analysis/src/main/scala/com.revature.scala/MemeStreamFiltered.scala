package com.revature.scala

import org.apache.log4j._
import org.apache.spark.sql.{SparkSession, DataFrameReader}
import org.apache.spark.sql.functions._
import org.apache.http.impl.client.HttpClients
import org.apache.http.client.config.{RequestConfig, CookieSpecs}
import org.apache.http.client.utils.URIBuilder
import org.apache.http.client.methods.HttpGet
import java.io.{BufferedReader, InputStreamReader, PrintWriter}
import org.apache.spark.sql.types.{LongType, StringType}
import java.nio.file.{Files, Paths}
import scala.concurrent.Future

object MemeStreamFiltered {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder
      .appName("Meme Stream Filtered")
      .master("local[*]")
      .getOrCreate()

    helloTweetStream(spark)
    //popularLang(spark)
    //bitcoinLang(spark)
    //muskLang(spark)
    //mostPopular(spark)
    //structuredStream(spark)

    spark.stop()
  }

  def helloTweetStream(spark: SparkSession): Unit = {
    import spark.implicits._

    val bearerToken = System.getenv(("TWITTER_BEARER_TOKEN"))

    import scala.concurrent.ExecutionContext.Implicits.global
    Future {
      tweetStreamToDir(
        bearerToken,
        queryString = "?tweet.fields=lang&place.fields=country"
      )
    }

    var start = System.currentTimeMillis()
    var filesFoundInDir = false
    while (!filesFoundInDir && (System.currentTimeMillis() - start) < 30000) {
      filesFoundInDir =
        Files.list(Paths.get("MemeStream")).findFirst().isPresent()
      Thread.sleep(500)
    }
    if (!filesFoundInDir) {
      println(
        "Error: Unable to populate tweetstream after 30 seconds.  Exiting.."
      )
      System.exit(1)
    }

    val staticDF = spark.read.json("MemeStream")

    staticDF.printSchema()

    val streamDF =
      spark.readStream.schema(staticDF.schema).json("MemeStream")

    val count =
      streamDF.groupBy("matching_rules.tag").count().sort(desc("count"))

    val query = count.writeStream
      .outputMode("complete")
      .format("console")
      .queryName("counts")
      .option("truncate", false)
      .start()

    query.awaitTermination()
  }

  def tweetStreamToDir(
      bearerToken: String,
      dirname: String = "MemeStream",
      linesPerFile: Int = 50,
      queryString: String = ""
  ) = {
    val httpClient = HttpClients.custom
      .setDefaultRequestConfig(
        RequestConfig.custom.setCookieSpec(CookieSpecs.STANDARD).build()
      )
      .build()
    val uriBuilder: URIBuilder = new URIBuilder(
      s"https://api.twitter.com/2/tweets/search/stream$queryString"
    )
    val httpGet = new HttpGet(uriBuilder.build())
    httpGet.setHeader("Authorization", s"Bearer $bearerToken")
    val response = httpClient.execute(httpGet)
    val entity = response.getEntity()
    if (null != entity) {
      val reader = new BufferedReader(
        new InputStreamReader(entity.getContent())
      )
      var line = reader.readLine()
      var fileWriter = new PrintWriter(Paths.get("tweetstream.tmp").toFile)
      var lineNumber = 1
      val millis = System.currentTimeMillis()
      while (line != null) {
        if (lineNumber % linesPerFile == 0) {
          fileWriter.close()
          Files.move(
            Paths.get("tweetstream.tmp"),
            Paths.get(
              s"$dirname/tweetstream-$millis-${lineNumber / linesPerFile}"
            )
          )
          fileWriter = new PrintWriter(Paths.get("tweetstream.tmp").toFile)
        }
        fileWriter.println(line)
        line = reader.readLine()
        lineNumber += 1
      }
    }
  }

//   //def helloTweetStream(spark: SparkSession): Unit = {

//     val bearerToken = System.getenv(("TWITTER_BEARER_TOKEN"))

//     tweetStreamToDir(bearerToken)
//  // }

  // //def tweetStreamToDir(
  //     bearerToken: String,
  //     dirname: String = "MemeStream",
  //     linesPerFile: Int = 100
  // ) = {
  //   val httpClient = HttpClients.custom
  //     .setDefaultRequestConfig(
  //       RequestConfig.custom.setCookieSpec(CookieSpecs.STANDARD).build()
  //     )
  //     .build()
  //   // Take our streaming data/parameters
  //   val uriBuilder: URIBuilder = new URIBuilder(
  //     "https://api.twitter.com/2/tweets/search/stream?tweet.fields=lang&place.fields=country"
  //   )
  //   val httpGet = new HttpGet(uriBuilder.build())
  //   httpGet.setHeader("Authorization", s"Bearer $bearerToken")
  //   val response = httpClient.execute(httpGet)
  //   val entity = response.getEntity()
  //   if (null != entity) {
  //     val reader = new BufferedReader(
  //       new InputStreamReader(entity.getContent())
  //     )
  //     var line = reader.readLine()
  //     while (line != null) {
  //       println(line)
  //       line = reader.readLine()
  //     }
  //   }
  // }

  // The most popular languages used for meme stocks
  def popularLang(spark: SparkSession): Unit = {

    import spark.implicits._
    val df = spark.read.json("MemeStream")

    // df.printSchema()
    // df.explain(true)
    // df.show(5)

    val lang = df
      .select("data.lang")
      .filter($"lang" =!= "und")
      .groupBy("lang")
      .count()

    // lang.printSchema()
    // lang.explain(true)
    // lang.show(5)

    val total = lang.agg(sum("count")).first()(0)

    val langProp = lang
      .select($"lang", round(($"count" / total), 4).as("proportion"))
      .filter($"proportion" > 0.001)
      .orderBy(desc("proportion"))

    println(s"The total number of language tags is: $total")
    langProp.printSchema()
    langProp.explain(true)
    langProp.show()
  }

  def bitcoinLang(spark: SparkSession): Unit = {

    import spark.implicits._
    val df = spark.read.json("MemeStream")

    val bitcoin = df
      .select("data.lang")
      .filter($"lang" =!= "und")
      .filter($"matching_rules.tag".cast(StringType) === "[bitcoin]")
      .groupBy("lang")
      .count()

    // bitcoin.printSchema()
    // bitcoin.explain(true)
    // bitcoin.show(5)

    val totalB = bitcoin.agg(sum("count")).first()(0)

    val propB = bitcoin
      .select($"lang", round(($"count" / totalB), 4).as("proportion"))
      .filter($"proportion" > 0.001)
      .orderBy(desc("proportion"))

    println(s"The total mentions of Bitcoin was: $totalB")
    propB.printSchema()
    propB.explain(true)
    propB.show()
  }

  // Finds the most proportion of languages talking about Elon Musk
  def muskLang(spark: SparkSession): Unit = {

    import spark.implicits._
    val df = spark.read.json("MemeStream")

    val langMusk = df
      .filter($"data.lang" =!= "und")
      .groupBy("data.lang")
      .count()

    // langMusk.printSchema()
    // langMusk.explain(true)
    // langMusk.show(5)

    val total = langMusk.agg(sum("count")).first()(0)

    val proportion = langMusk
      .select($"lang", round(($"count" / total), 4).as("proportion"))
      .filter($"proportion" > 0.001)
      .sort(desc("proportion"))

    println(s"Total number of Elon Musk mentions: $total")
    proportion.printSchema()
    proportion.explain(true)
    proportion.show()
  }

  // Finds the proportion of meme stock mentions
  def mostPopular(spark: SparkSession): Unit = {

    import spark.implicits._
    val df = spark.read.json("MemeStream")

    val count = df.groupBy("matching_rules.tag").count()

    count.show(5)

    val total = count.select("count").agg(sum("count")).first()(0)
    val proportion = count
      .select($"tag", round(($"count" / total), 4).as("proportion"))
      .filter($"proportion" > 0.01)
      .sort(desc("proportion"))

    println(s"The total number of mentions of meme stock was: $total")
    proportion.printSchema()
    proportion.explain(true)
    proportion.show(false)
  }

  // def structuredStream(spark: SparkSession): Unit = {

  //   val staticDF = spark.read.json("MemeStream")

  //   staticDF.printSchema()

  //   val streamDF =
  //     spark.readStream.schema(staticDF.schema).json("MemeStream")

  //   val count =
  //     streamDF.groupBy("matching_rules.tag").count().sort(desc("count"))

  //   val query = count.writeStream
  //     .outputMode("complete")
  //     .format("console")
  //     .queryName("counts")
  //     .option("truncate", false)
  //     .start()

  //   query.awaitTermination()
  // }
}
