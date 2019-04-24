package top

import java.{lang, util}
import java.util.{HashSet, Iterator}

import org.ansj.app.keyword.Keyword
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import top.Main.{NewsTemp, UserTemp, recommendations}
import top.feng.algorithms.RecommendKit
import top.feng.contentbasedrecommend.{ContentBasedRecommender, CustomizedHashMap, TFIDF}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer


/*
* 1.更新用户画像\
*   1.1. 获取用户画像数据（格式化用户兴趣标签数据）
*   1.2. 获取用户浏览数据
*   1.3. 根据用户画像和浏览历史更新各个用户的用户兴趣标签
*   1.4. 用户新的画像数据保存到对应的表中
* 2.使用新画像根据CB生成推荐
*   2.1 获取用户画像数据（格式化用户兴趣标签数据）
*   2.2 获取所有待推荐的新闻列表（格式化所有新闻对应的关键词及关键词的权重）
*   2.3 循环各用户，计算所有新闻跟用户的相关度
*   2.4 过滤（相似度为0，已经看过的，重复的，已经推荐过，截取固定数量的的新闻）
*   2.5 生成推荐列表
* */

object ContentBasedRecommenderNew {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.master("local[*]").appName("recommand-system").getOrCreate

    //2.1 获取用户画像数据（格式化用户兴趣标签数据）
    val userList = spark.read.textFile(args(0)).rdd.map(s=> {
      val tokens = s.split("::")

      UserTemp(tokens(0).toLong,tokens(1),Main.jsonPrefListtoMap(tokens(1)),tokens(2),tokens(3))
    })

    //2.2 获取所有待推荐的新闻列表（格式化所有新闻对应的关键词及关键词的权重）
    val newsList = spark.read.textFile(args(1)).rdd.map(line => {

      val tokens = line.split("::")

      var title = tokens(3)
      var content = ""
      val keywords: util.List[Keyword] = TFIDF.getTFIDE(title, content, 10)

      NewsTemp(tokens(0).toLong, "", tokens(2), title, tokens(4).toInt, tokens(5), keywords)

    }).collect()

    val newsBroadCast = spark.sparkContext.broadcast(newsList)

    import spark.implicits._
    val dataFrame = userList.map(user => {
      var tempMatchArr = new ArrayBuffer[(Long, Long, Double)]()
      var ite = newsBroadCast.value.iterator
      while (ite.hasNext) {
        val news = ite.next
        val newsId = news.id
        val moduleId = news.module_id

        if (null != user.prefListExtend.get(moduleId)) {
          val tuple: (Long, Long, Double) = (user.id, newsId, getMatchValue(user.prefListExtend.get(moduleId), news.keywords))
          tempMatchArr += tuple
        }
      }
      // 去除匹配值为0的项目,并排序
      var sortedTuples: ArrayBuffer[(Long, Long, Double)] = tempMatchArr.filter(tuple => tuple._3 > 0).sortWith(compare)

      if (sortedTuples.length > 0) {
        //暂时不操作
        //过滤掉已经推荐过的新闻
        //RecommendKit.filterReccedNews(toBeRecommended, user.id)
        //过滤掉用户已经看过的新闻
        //RecommendKit.filterBrowsedNews(toBeRecommended, user.id)
        //如果可推荐新闻数目超过了系统默认为CB算法设置的单日推荐上限数（N），则去掉一部分多余的可推荐新闻，剩下的N个新闻才进行推荐
      }
      if (sortedTuples.length > 10)
        sortedTuples = sortedTuples.take(10)
      sortedTuples
    }).flatMap(_.toList).map(tuple => {
      recommendations(tuple._1, tuple._2)
    }).toDF()

    dataFrame.show()

    spark.stop()

  }

  def getMatchValue(map:CustomizedHashMap[String, Double],list:util.List[Keyword]): Double ={

    var matchValue = 0D
    import scala.collection.JavaConversions._
    for (keyword <- list) {
      if (map.contains(keyword.getName)) matchValue += keyword.getScore * map.get(keyword.getName)
    }

    matchValue
  }

  def removeZeroItem(arr: ArrayBuffer[(Long, Double)]): Unit ={
    arr.filter(tuple=>tuple._2>0)
  }

  def compare(t1:(Long,Long,Double),t2:(Long,Long,Double)): Boolean ={
    t1._3 > t2._3
  }

}
