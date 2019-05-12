package com.mz.ex.spark

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}


class DatabaseJob(spark: SparkSession, jdbcParams: JdbcParams) {
  def run() = {
    import spark.implicits._

    spark.read
      .format("jdbc")
      .option("url", jdbcParams.url)
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", jdbcParams.table)
      .option("user", jdbcParams.user)
      .option("password", jdbcParams.password)
      .load()
      .createOrReplaceTempView("employees")

    val df = spark.sql("SELECT first_name, salary FROM employees WHERE id BETWEEN 1 AND 2")

    // The columns of a row in the result can be accessed by field index
    df.map(row => "salary: " + row(1)).show()

  }
}

object DatabaseJob {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("DatabaseJob").master("local").getOrCreate()

    val jdbcParams: JdbcParams = JdbcParams(
      "jdbc:mysql://192.168.99.100/foo",
      "employees",
      "root",
      "root")
    val job = new DatabaseJob(spark, jdbcParams)

    job.run()
  }
}

case class JdbcParams(url: String, table: String, user: String, password: String)

case class Employee(id: Long , name: String , salary: Double )