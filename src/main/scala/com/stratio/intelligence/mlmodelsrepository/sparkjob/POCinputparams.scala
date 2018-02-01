package com.stratio.intelligence.mlmodelsrepository.sparkjob

import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


object POCinputparams extends App{

  val log = Logger.getLogger(getClass.getName)
  val spark = SparkSession.builder.getOrCreate()

  log.info(s"Input arguments: ${args.toString}" )

  val rdd: RDD[String] = spark.sparkContext.parallelize( args.toSeq.mkString(" ").split(" ") )
  val rdd2: RDD[(String, Int)] = rdd.map(x => (x,1)).reduceByKey(_+_)

  rdd2.collect().foreach( x=> println(s"${x._1}: ${x._2}"))
  rdd2.collect().foreach( x=> log.info(s"${x._1}: ${x._2}"))

}
