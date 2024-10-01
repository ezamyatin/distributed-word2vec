package ru.vkontakte.mf.distributed

import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.storage.StorageLevel
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import ru.vkontakte.mf.local.{ItemData, Optimizer, Opts}
import ru.vkontakte.mf.pair.generator.BatchedGenerator
import ru.vkontakte.mf.pair.{LongPair, LongPairMulti, Partitioner}
import ru.vkontakte.mf.pair.generator.w2v.{Item2VecGenerator, Pos2NegGenerator, SamplingMode}

import java.util.Random
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters.{asJavaIteratorConverter, asScalaIteratorConverter}
import scala.util.Try

/**
 * @author ezamyatin
 * */
class LMF extends BaseLMF {

  private var minUserCount: Int = 1
  private var minItemCount: Int = 1
  override def gamma: Float = 1f / negative

  def setMinUserCount(minCount: Int): this.type = {
    require(minCount >= 0)
    this.minUserCount = minCount
    this
  }

  def setMinItemCount(minCount: Int): this.type = {
    require(minCount >= 0)
    this.minItemCount = minCount
    this
  }

  def fit(dataset: DataFrame): RDD[ItemData] = {
    assert(!((checkpointInterval > 0) ^ (checkpointPath != null)))
    import dataset.sparkSession.sqlContext.implicits._
    val sc = dataset.sparkSession.sparkContext

    val numExecutors = sc.getConf.get("spark.executor.instances").toInt
    val numCores = sc.getConf.get("spark.executor.cores").toInt
    val sent = cacheAndCount(dataset
      .select("user", "item", "rating")
      .as[(Long, Long, Float)].rdd
      .repartition(numExecutors * numCores / numThread))

    try {
      doFit(Right(sent))
    } finally {
      sent.unpersist()
    }
  }

  override protected def pairsFromRat(sent: RDD[(Long, Long, Float)], partitioner1: Partitioner, partitioner2: Partitioner, seed: Long): RDD[LongPairMulti] = {
    sent.mapPartitions(it => new BatchedGenerator(it
      .filter(e => partitioner1.getPartition(e._1) == partitioner2.getPartition(e._2))
      .map(e => new LongPair(partitioner1.getPartition(e._1), e._1, e._2, e._3))
      .asJava, partitioner1.getNumPartitions, true).asScala)
  }

  override protected def initializeFromRat(sent: RDD[(Long, Long, Float)]): RDD[ItemData] = {
    sent.flatMap(e => Seq((ItemData.TYPE_LEFT, e._1) -> 1L, (ItemData.TYPE_RIGHT, e._2) -> 1L))
      .reduceByKey(_ + _).filter(e => if (e._1._1 == ItemData.TYPE_LEFT) e._2 >= minUserCount else e._2 >= minItemCount)
      .mapPartitions { it =>
        val rnd = new Random()
        it.map { case ((t, w), n) =>
          rnd.setSeed(w.hashCode)
          new ItemData(t, w, n, Optimizer.initEmbedding(dotVectorSize, useBias, rnd))
        }
      }
  }
}
