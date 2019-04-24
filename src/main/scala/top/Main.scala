package top

import java.io.IOException
import java.text.SimpleDateFormat
import java.util
import java.util.{ArrayList, Date}

import org.ansj.app.keyword.Keyword
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.codehaus.jackson.JsonParseException
import org.codehaus.jackson.`type`.TypeReference
import org.codehaus.jackson.map.{JsonMappingException, ObjectMapper}
import top.feng.contentbasedrecommend.CustomizedHashMap
import top.feng.model.Users

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

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.master("local[*]").appName("recommand-system").getOrCreate

    //1.1. 获取用户画像数据（格式化用户兴趣标签数据）
    val userList = spark.read.textFile(args(0)).rdd.map(s=> {
      val tokens = s.split("::")
      val date1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(tokens(2))

      val users = new userExtend
      users.setId(tokens(0).toLong)
      users.setPrefList(tokens(1))
      users.setLatestLogTime(date1)
      users.setName(tokens(3))
      users
    })

    //userList.collect().foreach{println}

    //用户兴趣标签值衰减
    val userExtend = userList.map(user=>{

      //用于删除喜好值过低的关键词
      val keywordToDelete = new ArrayList[String]

      val map = user.getPrefListExtend()

      var baseAttenuationCoefficient = 0.9

      var times = intervalTime(user.getLatestLogTime,new Date())

      for(i<- 0 to times.toInt - 1) baseAttenuationCoefficient *= baseAttenuationCoefficient

      var newMap:CustomizedHashMap[Integer, CustomizedHashMap[String, Double]] = new CustomizedHashMap[Integer, CustomizedHashMap[String, Double]]

      val ite = map.keySet.iterator

      while (ite.hasNext){
        //用户对应模块的喜好不为空
        val moduleId = ite.next
        val moduleMap = map.get(moduleId)
        //N:{"X1":n1,"X2":n2,.....}
        if (moduleMap.toString != "{}") {
          val inIte = moduleMap.keySet.iterator
          while (inIte.hasNext) {
            val key = inIte.next
            //累计TFIDF值乘以衰减系数
            val result = moduleMap.get(key) * baseAttenuationCoefficient
            if (result < 10) keywordToDelete.add(key)
            moduleMap.put(key, result)
          }
        }
        import scala.collection.JavaConversions._
        for (deleteKey <- keywordToDelete) {
          moduleMap.remove(deleteKey)
        }
        //newMap.put()
        keywordToDelete.clear()
        newMap.put(moduleId,moduleMap)
      }

      UserTemp(user.getId,user.getPrefList,newMap,new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(user.getLatestLogTime),user.getName)

    })

    //1.2. 获取用户浏览数据
    val newsLogList = spark.read.textFile(args(1)).rdd.map(s => {
      val tokens = s.split("::")

      val date1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(tokens(3))


      val keywordsArr = tokens(8).split(",")

      val map: Map[String, Double] = keywordsArr.map(str => {
        val keyword = str.split(":")
        (keyword(0), keyword(1).toDouble)
      }).toMap

      NewsLog_Temp(tokens(1).toLong,tokens(2).toLong,tokens(3),tokens(4).toInt,tokens(5),tokens(6),tokens(7).toInt,map)
    }).groupBy(_.user_id).map(row => {
      val iterator = row._2.iterator
      var arr = new ArrayBuffer[NewsLog_Temp]()
      while (iterator.hasNext) {
        arr += iterator.next()
      }

      (row._1, arr.toArray)
    })

    val newsBroadCast = spark.sparkContext.broadcast(newsLogList.collectAsMap())

    import spark.implicits._
    val newUserPortrait: RDD[users] = userExtend.map(user => {
      val newsList: Array[NewsLog_Temp] = newsBroadCast.value.get(user.id).getOrElse(new Array[NewsLog_Temp](0))
      println("处理前的rateMap：" + user.prefListExtend.toString)
      if (newsList.length >0) {
        //1.3. 根据用户画像和浏览历史更新各个用户的用户兴趣标签
        for(news <- newsList){
          val rateMap: CustomizedHashMap[String, Double] = user.prefListExtend.get(news.module_id)
          //println("原始rateMap：" + rateMap)
          val keywordIte = news.map.toIterator
          while(keywordIte.hasNext){
            val name = keywordIte.next()._1
            val score = keywordIte.next()._2
            if(rateMap.containsKey(name))
              rateMap.put(name, rateMap.get(name) + score)
            else
              rateMap.put(name, score)
          }
          //println("处理后的rateMap：" + user.prefListExtend.get(news.module_id))
        }
      }
      println("处理后的rateMap：" + user.prefListExtend.toString)
      users(user.id,user.prefListExtend.toString,user.latest_log_time,user.name)
    })


    val dataFrame = newUserPortrait.toDF()

    dataFrame.write
      .format("jdbc")
      .option("url", "jdbc:mysql://master02:3306")
      .option("dbtable", "RecommenderSystem.users_temp")
      .option("user", "root")
      .option("password", "root")
      .mode(SaveMode.Overwrite)
      .save()

    println("----------------------用户画像更新成功--------------------------")


    spark.stop()

  }

  def jsonPrefListtoMap (srcJson: String): CustomizedHashMap[Integer, CustomizedHashMap[String, Double]]
  =
  {
    val objectMapper = new ObjectMapper
    var map = new CustomizedHashMap[Integer, CustomizedHashMap[String, Double]]
    try
      map = objectMapper.readValue(srcJson, new TypeReference[CustomizedHashMap[Integer, CustomizedHashMap[String, Double]]]() {})
    catch {
      case e: JsonParseException =>
        e.printStackTrace()
      case e: JsonMappingException =>
        // TODO Auto-generated catch block
        e.printStackTrace()
      case e: IOException =>
        e.printStackTrace()
    }
    return map
  }

  def intervalTime(startDate:Date,endDate:Date): Long ={
    var between = endDate.getTime - startDate.getTime
    val day: Long = between / 1000 / 3600 / 24
    day
  }

  class userExtend() extends Users(){

    var PrefListExtend:CustomizedHashMap[Integer, CustomizedHashMap[String, Double]] = null

    def getPrefListExtend(): CustomizedHashMap[Integer, CustomizedHashMap[String, Double]] ={
      PrefListExtend =jsonPrefListtoMap(this.getPrefList)

      PrefListExtend
    }
    def setPrefListExtend(s:CustomizedHashMap[Integer, CustomizedHashMap[String, Double]])={
      PrefListExtend = s
    }
  }

  case class UserTemp(id:Long, prefList:String,prefListExtend:CustomizedHashMap[Integer, CustomizedHashMap[String, Double]],latest_log_time:String,name:String)

  case class NewsLog_Temp(user_id:Long,news_id:Long,view_time:String,prefer_degree:Int,title:String,content:String,module_id:Int,map:Map[String, Double])

  case class users(id:Long, prefList:String,latest_log_time:String,name:String)

  case class NewsTemp(id:Long, content:String, news_time:String, title:String, module_id:Int, url:String, keywords:util.List[Keyword])

  case class recommendations(user_id:BigInt,news_id:BigInt)

}
