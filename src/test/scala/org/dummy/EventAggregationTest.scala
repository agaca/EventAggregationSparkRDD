package org.dummy

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.junit.Assert
import org.junit.Test

object EventAggregationTest{
  /**
   * Spark context for Unit Tests.
   */
  val sc = new SparkContext(new SparkConf(false)
    .setMaster("local").setAppName("Prueba"))
  val caseA: Array[String] = Array(
    "0:AAAAABAAAAABAAAAABAAAAAB",
    "1:AAAAABAAAAABAAAAABAAAAAB",
    "2:AAAAABAAAAAAAAAAABAAAAAB",
    "3:AAAAABAAAAABAAAAABAAAAAB",
    "4:AAAAABAAAAABAAAAABAAAAAB",
    "5:AAAAABBBBBBBAAAAABAAAAAB",
    "6:AAAAAAAAAAAAAAAAABAAAAAB",
    "7:AAAAABAAAAABAAAAABAAAAAB",
    "8:AAAAABAAAAABAAAAABAAAAAB"
  )
  val aggregation = new EventAggregation()
  val rdd = sc.parallelize(caseA)
}

class EventAggregationTest {



  @Test
 def dayWithMaxDurationTest(): Unit = {

    val dayWithMaxDuration = EventAggregationTest.aggregation
      .calculateDayWithMaxDuration(EventAggregationTest.rdd)
    Assert.assertEquals("The max duration for A is 6", 6l, dayWithMaxDuration("A"))
    Assert.assertEquals("The max duration for B is 5", 5l, dayWithMaxDuration("B"))

  }

   @Test
   def numberOfTransitionsTest(): Unit = {

     val numberOfTransitions = EventAggregationTest.aggregation
       .calculateNumberOfTransitions(EventAggregationTest.rdd)
     Assert.assertEquals("The number for AB is 32", 32l, numberOfTransitions(("A", "B")))
     Assert.assertEquals("The number for BA is 23", 23l, numberOfTransitions(("B", "A")))


   }

   @Test
   def totalDurationTest(): Unit = {


     val totalDuration = EventAggregationTest.aggregation
       .calculateTotalDuration(EventAggregationTest.rdd)
     Assert.assertEquals("The duration for A is 178", 178l, totalDuration("A"))
     Assert.assertEquals("The duration for B is 38", 38l, totalDuration("B"))


   }

  @Test
  def numberOfOccurrencesTest(): Unit = {
    val numberOfOccurrences = EventAggregationTest.aggregation
      .numberOfOccurrences(EventAggregationTest.rdd)
    Assert.assertEquals("The number of occurrences of A is 32", 32l, numberOfOccurrences("A"))
    Assert.assertEquals("The number of occurrences of B is 32", 32l, numberOfOccurrences("B"))

  }


}
