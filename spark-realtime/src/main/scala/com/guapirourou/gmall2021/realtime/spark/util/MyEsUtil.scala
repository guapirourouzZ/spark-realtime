package com.guapirourou.gmall2021.realtime.spark.util

import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}

import java.util.Properties

object MyEsUtil {
    private var factory: JestClientFactory = null

    def build() = {
        factory = new JestClientFactory
        val properties: Properties = PropertiesUtil.load("config.properties")
        val serverUrl: String = properties.getProperty("elasticsearch.server")
        factory.setHttpClientConfig(new HttpClientConfig.Builder("serverUrl")
                .multiThreaded(true).maxTotalConnection(20)
                .connTimeout(10000).readTimeout(10000).build())
    }

    def getClient: JestClient = {
        if(factory==null){
            build()
        }
        factory.getObject
    }

    def main(args: Array[String]): Unit = {
        val jest: JestClient = getClient
        jest.execute()
    }

}
