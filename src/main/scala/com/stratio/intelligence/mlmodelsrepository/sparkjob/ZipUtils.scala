package com.stratio.intelligence.mlmodelsrepository.sparkjob

import java.io.{File, FileInputStream, FileOutputStream}
import java.util.zip.{ZipEntry, ZipInputStream}

import org.apache.log4j.Logger
import resource._

object ZipUtils {

  private val log = Logger.getLogger(getClass.getName)

  def extract(zipFile: String, outputFolder: String, deleteAfterUnzip:Boolean=true ): Unit = {
    log.debug(s"****************************************************************")
    log.debug(s"Unziping file ${zipFile}")
    log.debug(s"****************************************************************")

    val source = new File(zipFile)
    val dest = new  File(outputFolder)
    dest.mkdirs()
    for(in <- managed(new ZipInputStream(new FileInputStream(source)))) {
      extract(in, dest)
    }
  }

  private def extract(in: ZipInputStream, dest: File): Unit = {
    dest.mkdirs()
    val buffer = new Array[Byte](1024 * 1024)

    var entry = in.getNextEntry
    while(entry != null) {

      log.debug("file unzip : " + entry.getName)
      if(entry.isDirectory) {
        new File(dest, entry.getName).mkdirs()
      } else {
        val filePath = new File(dest, entry.getName)
        for(out <- managed(new FileOutputStream(filePath))) {
          var len = in.read(buffer)
          while(len > 0) {
            out.write(buffer, 0, len)
            len = in.read(buffer)
          }
        }
      }
      entry = in.getNextEntry
    }
  }

}
