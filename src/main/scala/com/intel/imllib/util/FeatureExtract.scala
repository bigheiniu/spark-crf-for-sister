package com.intel.imllib.util
import java.io.File

import com.intel.imllib.crf.nlp.{Sequence, Token}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashMap => MHash, Map}
import scala.collection.immutable.{HashMap => ImHash}
import scala.io.Source

/**
  * 对 tokens 进行处理
  * 输入: table content; 一个完整的句子进行输入
  * */
object FeatureExtract {

  private val positive = "Y"
  private val negative = "N"

  private def corpus_data_contain_features(tableDict: ImHash[String, Array[String]], sequence: Array[String], fileTag: Int, id: Int): Array[ImHash[String, Any]] = {
    val features_list = new ArrayBuffer[MHash[String,Any]]()
    //词性判断可能需要专门的程序, 不能直接从文件中读取, 所以删除了这一步的内容
    val POS_list = {
      if (tableDict.contains("genia_taggered_train.txt")) tableDict("genia_taggered_train.txt")
      else if(tableDict.contains("genia_taggered_test.txt")) tableDict("genia_taggered_test.txt")
      else tableDict("genia_taggered_apply.txt")
    }
    //TODO: 词性这里出了一个 bug, 需要定位到哪一句, 然后再进行处理; => 目前 posfeature 是一个废的特征

    sequence.filter(_.nonEmpty).foreach(word =>  {
      word match {
        case _ => {
          val wordWithLabel = word.split("""\|""")
          try {
            val token = wordWithLabel(0)
            val features = mutable.HashMap[String, Any]("POS_feature" -> "None")
            features ++= {
            // TODO: 因为我是一句一句读, 所以省略了"sent_end_tag"
            //"BIEO" -> wordWithLabel(1)
              if (fileTag == 1) mutable.HashMap("id" -> "None", "token" -> token,"IOB2" -> wordWithLabel(1))
              else mutable.HashMap("id" -> "None", "token" -> token)
            }
            features_list.append(features ++= features_extract(token,tableDict))

          }
          catch {
            case _: Throwable => {
              println(s"the fucking is $id and the word is $word" )
            }
          }
        }}}
    )
    features_list.toArray.map(arr => ImHash(arr.toSeq:_*))
  }

  private def features_extract(token: String, table_dict: ImHash[String, Array[String]]): ImHash[String, String] = {
    val feature_dict = new MHash[String, String]()
    //# 往特征值字典中加入构词特征的18个子特征键值对（18）
    feature_dict ++= Word_structure_feature(token)
    //#词长度特征
    feature_dict += ("word_length" ->  word_length(token))

    //# 构建关键词特征（1）
    feature_dict += ("keywords_feature" -> Keywords_feature(token, table_dict))
    //# 构建词缀特征（4）
    feature_dict ++= Affix_feature(token, table_dict)
    //# 词形特征（2）
    feature_dict ++= Morphology_feature(token, table_dict)
    //# 边界词特征（2）
    feature_dict ++= Boundary_word_feature(token, table_dict)
    //# 一元词特征（1）
    feature_dict += ("Unary_feature" ->  Unary_feature(token, table_dict))
    //# 嵌套词特征（1）
    feature_dict += ("Nested_feature" -> Nested_feature(token, table_dict))
    //# 停用词特征（1）
    feature_dict += ("StopWord_feature" ->  StopWord_feature(token, table_dict))
    //# 通用词特征（1）
    feature_dict += ("CommenWord_feature" ->  CommenWord_feature(token, table_dict))
    //# 上下文特征（1）
    feature_dict += ("Context_feature" ->  Context_feature(token, table_dict))
    //# 词典特征（3）
    feature_dict ++= Dict_feature(token,table_dict)
    ImHash(feature_dict.toSeq:_*)
  }

  private def Word_structure_feature(token: String): ImHash[String, String] = {
    val word_structure_feature = new MHash[String, String]()

    //首字母大写特征
    word_structure_feature += ("CapWord" -> W_match_pattern("^[A-Z][a-z]+$",token))
    //全部大写字母
    word_structure_feature += ("AllCaps" -> W_match_pattern("^[A-Z]+$",token))
    //大小写字母组合
    word_structure_feature += ("CapsMix" -> W_match_pattern("^[A-Z]*([A-Z][a-z]|[a-z][A-Z])[A-z]*$",token))
    //字母数字交替组合
    word_structure_feature += ("AlphaDigitMix" -> W_match_pattern("^[A-Z0-9]*([A-z][0-9]|[0-9][A-z])[A-z0-9]*$",token))
    //字母数字顺序组合
    word_structure_feature += ("AlphaDigit" -> W_match_pattern("^[A-Z]+[0-9]+$",token))
    //罗马数字
    // word_structure_feature["Roman"]=W_match_pattern("^[ⅠⅡⅢⅣⅤⅥⅦⅧⅨⅩⅪⅫ]",token)
    //包含连字符
    word_structure_feature += ("Hyphen" -> W_match_pattern(".*[-].*",token))
    //以连字符开始
    word_structure_feature += ("InitHyphen" -> W_match_pattern("^[-].*",token))
    //以连字符结束
    word_structure_feature += ("EndHyphen"->W_match_pattern(".*[-]$",token))
    //停顿标点符号
    word_structure_feature += ("Punctuation"->W_match_pattern("[,.;:?!]$",token))
    //包含引号
    // word_structure_feature["Quote"->W_match_pattern("""[\""`][""]{2}|[``]{2}""",token)
    word_structure_feature += ("Quote" -> W_match_pattern(".*[\'].*", token))
    //希腊字母
    word_structure_feature += ( "GreekLetter"->W_match_pattern("""[αβγδεζηθικλμνξπρστυφχψω]|[alpha]{5}|[beta]{4}|[gamma]{5}|[delte]{5}|[\
      epsilon]{7}|[zeta]{4}|[eta]{3}|[theta]{5}|[iota]{4}|[kappa]{5}|[lambda]{6}|[mu]{2}|[nu]{2}|[xi]{2}|[omicron]{7}|[pi]⑵|[rho]{3}\
      |[sigma]{5}|[tau]{3}|[upsilon]{7}|[phi]{3}|[chi]{3}|[psi]{3}|[omega]{5}""",token))
    //大写字母
    word_structure_feature += ("UpperLetter"->W_match_pattern("^[A-Z]",token))
    //一位数字
    word_structure_feature += ("Numeral"->W_match_pattern("^[0-9]",token))
    //两位数字
    word_structure_feature+= ("TwoNumeral"->W_match_pattern("^[0-9][0-9]",token))
    //包含"/"
    word_structure_feature += ("ContainSlaslT"->W_match_pattern(".*[/].*",token))
    //左括号
    word_structure_feature += ("LeftMarkChar"->W_match_pattern("^[\\[(].*",token))
    //右括号
    word_structure_feature += ("RightMarkChar"->W_match_pattern(".*[\\])]$.*",token))
    //以点结束
    word_structure_feature += ("EndDot" -> W_match_pattern("^[A-z]*[.]$", token))
    // 1-2位字母
      word_structure_feature += ("OneOrTwoLetter" ->  W_match_pattern("^[A-z]{1,2}$", token))
    // 全部数字
    word_structure_feature += ("AllDigit" -> W_match_pattern("^[0-9]+$", token))
    // 包含"="
    word_structure_feature += ("Equal" -> W_match_pattern(".*[=][A-z]*", token))
    // 下划线
    word_structure_feature += ("Underline" -> W_match_pattern("[\\w]*_[\\w]*", token))
    // +号
    word_structure_feature += ("Plus" -> W_match_pattern("[\\w]*[+][\\w]*", token))
    ImHash(word_structure_feature.toSeq:_*)
  }

  private def W_match_pattern(pattern: String,token: String): String = {
    val patternRe = pattern.r
    if (patternRe.findAllIn(source = token).nonEmpty)
      positive
    else negative
  }
  //#判断给定词是否在给定文件中的词构成的列表中出现,是则返回'Y'，否则'N'
  private def query_W_in_Table(token: String, file_content: Array[String]): String = {
    if (file_content.contains(token))
      positive
    else negative
  }
  //#词长度特征word_length
  private def word_length(token: String): String = {
    val len = token.length
    if (len == 0) println("the token is:" + token +"is")
    require(len > 0,"the token's length should longer than 0")
    len match {
      case 1 => "1"
      case 2 => "2"
      case 3 => "3"
      case 4 => "3"
      case 5 => "3"
      case _ => "4"
    }
  }
  //#关键词特征(关键词词表事先做好)，1个
  private def Keywords_feature(token:String, table_dict: ImHash[String, Array[String]]): String = {
    val files_content = table_dict("keyWordTable.txt")
    query_W_in_Table(token,files_content)
  }
  //词缀特征,返回一个有4个特征键值对的字典
  private def Affix_feature(token: String, table_dict: ImHash[String, Array[String]]): ImHash[String, String] = {
    val affix_feature = new MHash[String, String]()
    val len = token.length
    affix_feature += ("prefix3" -> query_W_in_Table(token.slice(0,3),table_dict("prefix3Table.txt")))
    affix_feature += ("prefix4" -> query_W_in_Table(token.slice(0,4),table_dict("prefix4Table.txt")))
    affix_feature += ("suffix3" -> query_W_in_Table(token.slice(len-3,len),table_dict("suffix3Table.txt")))
    affix_feature += ("suffix4" -> query_W_in_Table(token.slice(len-4,len),table_dict("suffix4Table.txt")))
    ImHash(affix_feature.toSeq:_*)
  }
  //词形特征,返回一个有2个特征键值对的字典
  // 这个指标存在差值, 因为 python 是"in"进行 substring 比较, 是相似; scala 使用的是 equals, 需要一模一样才行
  private def Morphology_feature(token: String, table_dict: ImHash[String,Array[String]]): ImHash[String, String] = {
    val morphology_feature = new mutable.HashMap[String, String]()
    var morphword = token.replaceAll("[A-Z]","A") //用'A'替换token中所有大写字母
    morphword = morphword.replaceAll("[a-z]","a") //用'a'替换token中所有小写字母
    morphword= morphword.replaceAll("\\d","0") //用'0'替换token中所有数字
    morphword= morphword.replaceAll("[^A-Za-z\\d]","x") //用'x'替换token中所有非字母数字字符
    morphology_feature += ("morph" -> query_W_in_Table(morphword,table_dict("morph_listTable.txt")))
    val s_morphword = morphword.replaceAll("(\\w)\\1+","$1") //#把连续相同的字符合并
    morphology_feature += ("s_morph" -> query_W_in_Table(s_morphword, table_dict("s_morph_listTable.txt")))//#简单词形特征
    ImHash(morphology_feature.toSeq:_*)
  }

  //#边界词特征,返回一个有2个特征键值对的字典
  private def Boundary_word_feature(token: String,table_dict: ImHash[String,Array[String]]): ImHash[String, String] = {
   val boundary_word_feature = new mutable.HashMap[String, String]()
    boundary_word_feature += ("left_boundary" -> query_W_in_Table(token, table_dict("left_boundaryTable.txt")))
    boundary_word_feature +=  ("right_boundary" -> query_W_in_Table(token, table_dict("right_boundaryTable.txt")))
    ImHash(boundary_word_feature.toSeq:_*)
  }

  //#一元词特征，1个
  private def Unary_feature(token:String,table_dict: ImHash[String, Array[String]]): String = query_W_in_Table(token, table_dict("unaryTable.txt"))

  //#嵌套词特征，1个
  private def Nested_feature(token: String,table_dict: ImHash[String,Array[String]]): String = query_W_in_Table(token,table_dict("nestedTable.txt"))

  //#停用词特征，1个
  private def StopWord_feature(token: String, table_dict: ImHash[String,Array[String]]): String = query_W_in_Table(token,table_dict("stopTable.txt"))

  //#通用词特征，1个
  private def CommenWord_feature(token: String, table_dict: ImHash[String,Array[String]]): String = query_W_in_Table(token,table_dict("commonTable.txt"))

  //#上下文特征，1个
  private def Context_feature(token: String, table_dict: ImHash[String,Array[String]]): String = query_W_in_Table(token,table_dict("contextTable.txt"))

  //#词典特征，返回一个有3个特征键值对的字典
  private def Dict_feature(token: String, table_dict: ImHash[String,Array[String]]): ImHash[String,String] = {
    val dict_feature = new mutable.HashMap[String,String]()
    dict_feature += ("dict" -> query_W_in_Table(token, table_dict("bacteria_dict_Table.txt"))) // #词典单词特征
    dict_feature += ("dict_U" -> query_W_in_Table(token, table_dict("bacteria_dict_U_Table.txt"))) //#词典一元词特征
    dict_feature += ("dict_N" -> query_W_in_Table(token, table_dict("bacteria_dict_N_Table.txt"))) //#词典嵌套词特征
    ImHash(dict_feature.toSeq: _*)
  }

  // TODO: 需要换一个地方, 放在程序外面
   def get_table_content(dir: String): ImHash[String, Array[String]] = {
   val table_dict = new mutable.HashMap[String, Array[String]]()
     val fileStream = getClass.getResourceAsStream("/filename.txt")
     val lines = Source.fromInputStream(fileStream).getLines()
     lines.foreach(
         file => {
           println(file.toString)
           var i = 0
           val POSlist = new ArrayBuffer[String]()
           if (file.contains("genia_taggered")) i = 2
           else if (file.contains("Mostfrequentlywords")) i = 1
           Source.fromInputStream(getClass.getResourceAsStream("/" + file)).getLines().filter(_.nonEmpty).foreach(arr => POSlist.append(arr.split("\t")(i)))
           table_dict.put(file,POSlist.toArray)
         }
       )
    ImHash(table_dict.toSeq: _*)
  }
  //返回 words|tags|labels =>

  private def convert(array: Array[ImHash[String,Any]],fileTag: Int):Sequence = {
    val par = Parameter()
    val featureNameList = par.feature_name_list
    val sequence = array.map(arr => {
      val Mtags = ArrayBuffer[Any]()
      featureNameList.foreach(name => Mtags.append(arr(name)))
      val tags = Mtags.toArray.map(_.toString)
      if (fileTag == 1) Token.put(arr("IOB2").toString,tags)
      else {
        Token.put(tags)
      }
    })
    Sequence(sequence)
  }

  // TODO: 大小写转换问题
  def getSequece(arrayTokens: Array[String],table: ImHash[String, Array[String]],fileTag: Int,Index: Int): Sequence =
      convert(corpus_data_contain_features(table,arrayTokens,fileTag,Index),fileTag)


//  def main(args: Array[String]): Unit = {
//    import scala.io.Source
//    val result = Source.fromFile("/Users/bigheiniu/course/graduate_pro/spark-crf/spar-crf-data/microProject/corpus/testResult.txt").getLines().map(x => x.split("\t").filter(_.)).toArray
//    val tableDir = "/Users/bigheiniu/course/graduate_pro/spark-crf/spar-crf-data/BNER/table"
//    val tablContent = get_table_content(tableDir)
//    val fuckTest = result.map(arr => getSequece(arr,tablContent,0,0))
//
//    def bitchTest = Source.fromFile("/Users/bigheiniu/course/graduate_pro/spark-crf/imllib-spark/data/crf/raw/test.txt").getLines().filter(_.nonEmpty).map(Sequence.deSerializer).toArray
//    val newBitch = bitchTest.map(arr => Sequence(arr.toArray.map(arr1 => Token.put(arr1.tags))))
//
//    fuckTest.zip(newBitch).foreach(arr => {
//      if (!arr._1.compareSe(arr._2)) println("fuck")
//    })
//
//  }
}
