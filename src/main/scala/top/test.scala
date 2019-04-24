package top

import java.util
import org.ansj.app.keyword.Keyword
import org.apache.spark.sql.{SaveMode, SparkSession}
import top.feng.contentbasedrecommend.TFIDF

object test {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate

    import spark.implicits._
    val dataFrame = spark.read.textFile("E:/test/news.csv").rdd.map(line => {

      //val news = new NewsTemp

      val tokens = line.split("::")

      var title = tokens(3)
      var content = ""
      val keywords: util.Iterator[Keyword] = TFIDF.getTFIDE(title, content, 10).iterator()


      var str = ""
      while (keywords.hasNext) {
        var keyword = keywords.next()
        val name = keyword.getName
        val score = keyword.getScore
        //keywords.next().getName
        str += name + ":" + score + ","
      }

      if (str.endsWith(",")) str = str.substring(0, str.length - 1)

      news(tokens(0).toLong, "", tokens(2), title, tokens(4).toInt, tokens(5), str)

    }).toDF()
    dataFrame.write
      .format("jdbc")
      .option("url", "jdbc:mysql://master02:3306")
      .option("dbtable", "RecommenderSystem.news_temp")
      .option("user", "root")
      .option("password", "root")
      .mode(SaveMode.Append)
      .save()


    println("--------------------数据保存成功......！------------------------")


    spark.stop()


  }
  case class news(id:Long, content:String, news_time:String, title:String, module_id:Int, url:String, keywords:String)
}
