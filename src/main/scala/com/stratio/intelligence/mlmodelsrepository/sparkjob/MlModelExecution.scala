package com.stratio.intelligence.mlmodelsrepository.sparkjob

import java.io.File

import com.stratio.intelligence.mlmodelsrepository.sparkjob.POCinputparams.{args, getClass, log}
import org.apache.http.client.HttpClient
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClients
import org.apache.http.ssl.SSLContextBuilder
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

import scala.annotation.tailrec


object MlModelExecution extends App{

  val log = Logger.getLogger(getClass.getName)

  // => Creating sparksession
  val spark = SparkSession.builder.getOrCreate()
  val sparkconf = spark.sparkContext.getConf

  // => Parse input args
  log.info(s"Input arguments: ${args.toString}" )

  var hdfsInput = ""
  var hdfsOutput = ""
  args.sliding(2, 2).toList.collect {
    case Array("--in", in: String) => hdfsInput = in
    case Array("--out", out: String) => hdfsOutput = out
    case _ => throw new IllegalArgumentException()
  }

  // => Download model from ML repo
  def generateSecureClient(cassPass: Seq[(String, String)]): HttpClient = {
    val sslContext = SSLContextBuilder.create()
    @tailrec
    def loadCa(cassPass: Seq[(String, String)], result: SSLContextBuilder): SSLContextBuilder = {
      if (cassPass.isEmpty) result
      else {
        val (caFileName, caPass) = cassPass.head
        val caFile = new File(caFileName)
        val actualRes = result.loadTrustMaterial(caFile, caPass.toCharArray)
        loadCa(cassPass.tail, actualRes)
      }
    }
    HttpClients.custom().setSSLContext(loadCa(cassPass, sslContext).build()).build
  }
  val sparkSSLprefix = "spark.ssl.datastore."
  val configSSL: Map[String, String] = sparkconf.getAllWithPrefix(sparkSSLprefix).toMap
  val certs: Seq[(String, String)] = Seq( (configSSL("keyStore"), configSSL("keyStorePassword")),
                                          (configSSL("trustStore"), configSSL("trustStorePassword")) )
  val secureHttpClient: HttpClient = generateSecureClient(certs)
  val get = new HttpGet("")



}