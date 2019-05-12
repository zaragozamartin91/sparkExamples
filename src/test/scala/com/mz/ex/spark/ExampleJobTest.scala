package com.mz.ex.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{After, Before, Test}
import org.scalatest.junit.AssertionsForJUnit

class ExampleJobTest extends AssertionsForJUnit {
  var sc: SparkContext = _

  @Before
  def initialize() {
    val conf = new SparkConf().setAppName("SparkJoins").setMaster("local")
    sc = new SparkContext(conf)
  }

  @After
  def tearDown() {
    sc.stop()
  }

  @Test
  def testExampleJobCode() {
    val job = new ExampleJob(sc)
    val result = job.run("./transactions.txt", "./users.txt")

    assert(result.collect().length === 2)
  }
}
