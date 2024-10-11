package ru.vk.factorization.logistic.distributed

import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.storage.StorageLevel
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import ru.vk.factorization.logistic.local.{ItemData, Optimizer, Opts}
import ru.vk.factorization.logistic.pair.generator.BatchedGenerator
import ru.vk.factorization.logistic.pair.generator.w2v.{Item2VecGenerator, Pos2NegGenerator, SamplingMode}
import ru.vk.factorization.logistic.pair.{LongPair, LongPairMulti, Partitioner}

import java.util.Random
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters.{asJavaIteratorConverter, asScalaIteratorConverter}
import scala.util.Try

/**
 * @author ezamyatin
 */
class LMF extends BaseLMF[(Long, Long, Float)] {

  private var minUserCount: Int = 1
  private var minItemCount: Int = 1
  private var useImplicitPref: Boolean = true

  override protected def implicitPref: Boolean = useImplicitPref
  override protected def gamma: Float = 1f / negative

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

  def setImplicitPref(implicitPref: Boolean): this.type = {
    this.useImplicitPref = implicitPref
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
      doFit(sent)
    } finally {
      sent.unpersist()
    }
  }

  override protected def pairs(sent: RDD[(Long, Long, Float)], partitioner1: Partitioner, partitioner2: Partitioner, seed: Long): RDD[LongPairMulti] = {
    sent.mapPartitions(it => new BatchedGenerator(it
      .filter(e => partitioner1.getPartition(e._1) == partitioner2.getPartition(e._2))
      .map(e => new LongPair(partitioner1.getPartition(e._1), e._1, e._2,
        if (!useImplicitPref) e._3 else Float.NaN, if (useImplicitPref) e._3 else Float.NaN))
      .asJava, partitioner1.getNumPartitions, !useImplicitPref, implicitPref).asScala)
  }

  override protected def initialize(sent: RDD[(Long, Long, Float)]): RDD[ItemData] = {
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
