package com.stratio.intelligence.mlmodelsrepository.sparkjob

import java.io.{File, FileInputStream}
import java.security.KeyStore

import org.apache.http.impl.client.HttpClients
import org.apache.http.ssl.SSLContextBuilder


case class Cert(path:String, pass:String)

object SecureHttpClient {

  def get(keyStoreDef:Cert, truststoreDef:Cert ) = {

    // · Create keystore and trustStore objects
    val keyStore = KeyStore.getInstance(KeyStore.getDefaultType())
    keyStore.load(new FileInputStream(new File(keyStoreDef.path)), keyStoreDef.pass.toCharArray())
    val trustStore = KeyStore.getInstance(KeyStore.getDefaultType())
    trustStore.load(new FileInputStream(new File(truststoreDef.path)), truststoreDef.pass.toCharArray())

    // · Generate sslcontext
    val sslContext = new SSLContextBuilder()
      .useProtocol("TLSv1.2")
      .loadTrustMaterial(trustStore, null)
      .loadKeyMaterial(keyStore, keyStoreDef.pass.toCharArray()).build()

    HttpClients.custom().setSSLContext(sslContext).build
  }

}
