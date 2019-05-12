package com.mz.ex.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.Map

class ExampleJob(sc: SparkContext) {
  // reads data from text files and computes the results. This is what you test
  def run(t: String, u: String) : RDD[(String, String)] = {
    val transactions: RDD[String] = sc.textFile(t)
    val newTransactionsPair: RDD[(Int, Int)] = transactions.map{ t =>
      val p = t.split(" ")
      (p(2).toInt, p(1).toInt)
    }

    val users: RDD[String] = sc.textFile(u)
    val newUsersPair: RDD[(Int, String)] = users.map{ t =>
      val p = t.split(" ")
      (p(0).toInt, p(3))
    }

    val result = processData(newTransactionsPair, newUsersPair)
    sc.parallelize(result.toSeq).map(t => (t._1.toString, t._2.toString))
  }

  def processData (t: RDD[(Int, Int)], u: RDD[(Int, String)]) : Map[Int,Long] = {
    val jn = t.leftOuterJoin(u).values.distinct
    jn.countByKey
  }
}

object ExampleJob {
  def main(args: Array[String]) {
//    val transactionsIn = args(1)
//    val usersIn = args(0)



    val transactionsIn = "./transactions.txt"
    val usersIn = "./users.txt"

    val sparkSession = SparkSession.builder().appName("SparkJoins").master("local").getOrCreate()

    val context = sparkSession.sparkContext
    val job = new ExampleJob(context)
    val results: RDD[(String, String)] = job.run(transactionsIn, usersIn)

    val tuples: Array[(String, String)] = results.collect()
    tuples.foreach(t => println(s"${t._1} : ${t._2}"))
    context.stop()
  }
}