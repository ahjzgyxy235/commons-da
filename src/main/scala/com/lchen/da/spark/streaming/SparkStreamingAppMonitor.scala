package com.lchen.da.spark.streaming

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.streaming.{StreamingContext, StreamingContextState}
import org.slf4j.LoggerFactory

import scala.util.control.Breaks._

/**
  * Spark streaming application monitor
  *
  * usage:
  *     ssc.start()
  *     new SparkStreamingAppMonitor(ssc).start() // start monitor thread
  *     ssc.awaitTermination()
  *
  * Created by hzchenlei1.
  */
class SparkStreamingAppMonitor(private val ssc:StreamingContext ) extends Thread {

    private lazy val LOG = LoggerFactory.getLogger(classOf[SparkStreamingAppMonitor])

    var fs:FileSystem = FileSystem.get(ssc.sparkContext.hadoopConfiguration)
    // stop marker must be a hdfs file
    val shutdownMarkerPath:String = ssc.sparkContext.getConf.get("spark.streaming.gracefullyStopMarker")

    val checkIntervalMillis = 10000
    var isAppRunning = true
    var isAppShouldStop = false

    override def run(): Unit = {

        LOG.info("Spark streaming app monitor start to work")

        // break loop and end this monitor thread for the case that a user might stop application
        // forced by `yarn application -kill appid`
        breakable {
            while(isAppRunning){
                if(null==ssc || ssc.getState().equals(StreamingContextState.STOPPED)){
                    LOG.info("Streaming context state: {}", StreamingContextState.STOPPED)
                    break
                } else {
                    isAppShouldStop = fs.exists(new Path(shutdownMarkerPath)) && fs.isFile(new Path(shutdownMarkerPath))
                    if (isAppShouldStop) {
                        LOG.info("Found the shutdown marker, stop streaming application right now")
                        fs.delete(new Path(shutdownMarkerPath), false)
                        val stopSparkContext = ssc.sparkContext.getConf.getBoolean("spark.streaming.stopSparkContextByDefault", true)
                        val stopGracefully = ssc.sparkContext.getConf.getBoolean("spark.streaming.stopGracefullyOnShutdown", true)
                        ssc.stop(stopSparkContext, stopGracefully)
                        isAppRunning = false
                        LOG.info("Streaming application is stopped")
                    } else {
                        LOG.info("Spark streaming application name: {}, isRunning: {}",
                            ssc.sparkContext.getConf.get("spark.app.name", "Unknown name Application"), isAppRunning)
                        Thread.sleep(checkIntervalMillis)
                    }
                }
            }
        }
    }

}
