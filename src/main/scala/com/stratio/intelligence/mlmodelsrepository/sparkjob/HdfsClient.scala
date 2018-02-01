package com.stratio.intelligence.mlmodelsrepository.sparkjob

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger

object HdfsClient {

  private val log = Logger.getLogger(getClass.getName)

  lazy val hdfsFs: FileSystem = {
    val conf = new Configuration()
    FileSystem.get(conf)
  }

  def uploadFolder(localPath:String, hdfsPath:String, deleteAfterUpload:Boolean=true) = {
    log.debug(s"Uploading local folder '${localPath}' to '${hdfsPath}' hdfs path")

    hdfsFs.copyFromLocalFile( new Path(localPath), new Path(hdfsPath))
    if(deleteAfterUpload) {
      log.debug(s"Deleting local folder '${localPath}'")
      FileUtils.deleteDirectory(new File(localPath))
    }
  }

  def deleteFolder(folderPath:String): Unit ={
    if(checkIfExist(folderPath))
      hdfsFs.delete(new Path(folderPath), true)
  }

  def checkIfExist(path:String): Boolean ={
    hdfsFs.exists(new Path(path))
  }

}
