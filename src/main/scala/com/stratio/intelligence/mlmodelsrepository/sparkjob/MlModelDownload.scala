package com.stratio.intelligence.mlmodelsrepository.sparkjob

import java.io.FileOutputStream
import java.nio.file.{Files, Paths}

import org.apache.http.client.methods.HttpGet
import org.apache.log4j.Logger
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.{DataFrame, SparkSession}


object MlModelDownload extends App{

  val log = Logger.getLogger(getClass.getName)

  // => Creating sparksession
  log.info("----------------------------------------------------------------------------")
  log.info("Creating sparksession")
  log.info("----------------------------------------------------------------------------")

  val spark = SparkSession.builder.getOrCreate()
  val sparkconf = spark.sparkContext.getConf

  // => Download model from ML repo
  log.info("----------------------------------------------------------------------------")
  log.info("Download model from ML repo")
  log.info("----------------------------------------------------------------------------")


  val modelZipFile = downloadModel()

  // => Unzip model
  log.info("----------------------------------------------------------------------------")
  log.info("Unzip model")
  log.info("----------------------------------------------------------------------------")

  ZipUtils.extract(modelZipFile, "/tmp/model", true)

  // => Upload model to hdfs
  log.info("----------------------------------------------------------------------------")
  log.info("Upload model to hdfs")
  log.info("----------------------------------------------------------------------------")

  val hdfsOutputFolder = s"${sparkconf.get("spark.mlmodelrepository.outputHdfsFolder")}"
  HdfsClient.uploadFolder("/tmp/model", hdfsOutputFolder, false)

  // => Load model from hdfs
  log.info("----------------------------------------------------------------------------")
  log.info("Load model from hdfs")
  log.info("----------------------------------------------------------------------------")

  val pipelineModel = PipelineModel.load(s"$hdfsOutputFolder/model")

  // => Deleting model uploaded to hdfs
  log.info("----------------------------------------------------------------------------")
  log.info("Deleting model uploaded to hdfs")
  log.info("----------------------------------------------------------------------------")

  HdfsClient.deleteFolder(s"$hdfsOutputFolder/model")

  // => Reading input data
  log.info("----------------------------------------------------------------------------")
  log.info("Reading input data")
  log.info("----------------------------------------------------------------------------")

  val hdfsInputParquetFile = sparkconf.get("spark.mlmodelrepository.inputHdfsParquetFile")
  val inputDataframe: DataFrame = spark.read.parquet(hdfsInputParquetFile)
  
  // => Transforming input data
  log.info("----------------------------------------------------------------------------")
  log.info("Transforming input data")
  log.info("----------------------------------------------------------------------------")

  val outputDataframe = pipelineModel.transform(inputDataframe)
  
  // => Saving transformed data
  log.info("----------------------------------------------------------------------------")
  log.info("Saving transformed data")
  log.info("----------------------------------------------------------------------------")

  outputDataframe.write.parquet(s"$hdfsOutputFolder/transformeddata")

  
  def downloadModel():String = {

    // => Create secure Http client

    // · Obtaining certificates for tls connection from Spark
    val sparkConfigSSL: Map[String, String] = sparkconf.getAllWithPrefix("spark.ssl.datastore.").toMap
    val keystoreCert = Cert( sparkConfigSSL("keyStore"),sparkConfigSSL("keyStorePassword") )
    val truststoreCert = Cert( sparkConfigSSL("trustStore"), sparkConfigSSL("trustStorePassword") )

    log.info(s"-----------------------------------------------------------------------------------------------")
    log.info(s"Certs: ${keystoreCert} ${truststoreCert}")
    log.info( s" keystore downloaded: ${Files.exists(Paths.get(sparkConfigSSL("keyStore")))}")
    log.info( s" trustStore downloaded: ${Files.exists(Paths.get(sparkConfigSSL("trustStore")))}")
    log.info(s"-----------------------------------------------------------------------------------------------")

    // · Secure http client creation
    val secureHttpClient = SecureHttpClient.get(keystoreCert, truststoreCert)


    // => Request to ML model repository for downloading model

    // · Ml model repository url
    val mlModelRepositoryUrl = sparkconf.get("spark.mlmodelrepository.url")
    val mlModelRepositoryPort = sparkconf.get("spark.mlmodelrepository.port")
    val mlModelRepositoryModel = sparkconf.get("spark.mlmodelrepository.modelname")
    val downloadRequestUrl = s"https://${mlModelRepositoryUrl}:${mlModelRepositoryPort}/model/${mlModelRepositoryModel}"

    log.info(s"-----------------------------------------------------------------------------------------------")
    log.info(s"Request to Ml model repository: ${downloadRequestUrl}")
    log.info(s"-----------------------------------------------------------------------------------------------")

    val tmpFile = "/tmp/model.zip"
    val response = secureHttpClient.execute(new HttpGet(downloadRequestUrl))

    if( response.getStatusLine.getStatusCode == 200) {
      val entity = response.getEntity
      if (entity != null) {
        val outstream = new FileOutputStream(tmpFile)
        entity.writeTo(outstream)
        outstream.close()
      }
    }else{
      System.exit(1)
    }

    log.info(s"-----------------------------------------------------------------------------------------------")
    log.info( s" File downloaded: ${Files.exists(Paths.get(tmpFile))}")
    log.info(s"-----------------------------------------------------------------------------------------------")

    tmpFile
  }

}