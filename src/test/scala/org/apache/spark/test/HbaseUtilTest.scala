package org.apache.spark.test

import org.apache.spark.hbase.core.SparkHBaseContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.hadoop.hbase.client.Scan
import org.apache.spark.rdd.RDD
import java.util.UUID
import org.apache.hadoop.hbase.util.MD5Hash
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.hbase.core.HbaseConnectionCache
import scala.collection.mutable.ArrayBuffer
import org.apache.hadoop.hbase.client.Put
import java.util.ArrayList
import java.util.Random

object HbaseUtilTest {
  val tablename = "test"
  val zk = "solr1,solr2,datanode37"
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("tets")
    val sc = new SparkContext(conf)
    //val hc = new SparkHBaseContext(sc, sc.hadoopConfiguration)
    //put1YitoHbase(sc)
  }
  def put1YitoHbase(sc: SparkContext) {
    val rdd = sc.parallelize(1 to 40, 24)
    rdd.mapPartitionsWithIndex{ case(index,itor) =>
      val time=if(index>10) index.toString else "0"+index
      val table = HbaseConnectionCache.getTable("", "")
      val puts = new ArrayList[Put]
      var putsize = 0
      var allsize = 5000000
      while (allsize > 0) {
        val gid = MD5Hash.getMD5AsHex(Bytes.toBytes(UUID.randomUUID().toString()))
        val put = new Put(gid.getBytes)
        put.addColumn("info".getBytes, "time".getBytes, time.getBytes)
        puts.add(put)
        putsize+=1
        allsize-=1
        if (putsize == 10000) {
          table.put(puts)
          putsize = 0
          puts.clear()
        }
      }
      Array.emptyLongArray.toIterator
    }.count

  }

  def testBulkRDD(hc: SparkHBaseContext) = {
    hc.bulkAllRDD(zk, tablename, f)
  }
  def testBulkGet(hc: SparkHBaseContext, rdd: RDD[String]) {
    val r1 = hc.bulkGetRDD(zk, tablename, rdd, makeGet, convertResult)
  }
  def testBulkScan(hc: SparkHBaseContext) {
    val r2 = hc.bulkScanRDD(zk, tablename, new Scan(), f)
  }

}