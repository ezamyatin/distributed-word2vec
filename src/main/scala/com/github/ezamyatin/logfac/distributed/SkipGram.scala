package com.github.ezamyatin.logfac.distributed

import com.github.ezamyatin.logfac.local.{ItemData, Optimizer}
import com.github.ezamyatin.logfac.pair.{LongPairMulti, Partitioner}
import com.github.ezamyatin.logfac.pair.generator.BatchedGenerator
import com.github.ezamyatin.logfac.pair.generator.w2v.{Item2VecGenerator, Pos2NegGenerator, SamplingMode, WindowGenerator}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

import java.util.Random
import scala.jdk.CollectionConverters.{asJavaIteratorConverter, asScalaIteratorConverter}

/**
 * @author ezamyatin
 */
class SkipGram extends BaseLMF[Array[Long]] {

  private var minCount: Int = 1
  private var window: Int = 1
  private var samplingMode: SamplingMode = _

  def setWindowSize(window: Int): this.type = {
    require(window > 0,
      s"Window of words must be positive but got ${window}")
    this.window = window
    this
  }

  def setSamplingMode(samplingMode: String): this.type = {
    this.samplingMode = SamplingMode.valueOf(samplingMode)
    this
  }

  def setMinCount(minCount: Int): this.type = {
    require(minCount >= 0)
    this.minCount = minCount
    this
  }

  override def fit(dataset: DataFrame): RDD[ItemData] = {
    assert(!((checkpointInterval > 0) ^ (checkpointPath != null)))

    import dataset.sparkSession.sqlContext.implicits._
    val sc = dataset.sparkSession.sparkContext

    val numExecutors = sc.getConf.get("spark.executor.instances").toInt
    val numCores = sc.getConf.get("spark.executor.cores").toInt
    val sent = cacheAndCount(dataset
      .select("sequence")
      .as[Array[Long]].rdd
      .repartition(numExecutors * numCores / numThread))

    try {
      doFit(sent)
    } finally {
      sent.unpersist()
    }
  }

  override protected def pairs(sent: RDD[Array[Long]], partitioner1: Partitioner, partitioner2: Partitioner, seed: Long): RDD[LongPairMulti] = {
    sent.mapPartitionsWithIndex({ case (idx, it) =>
      new BatchedGenerator({
        if (samplingMode == SamplingMode.ITEM2VEC) {
          new Item2VecGenerator(it.asJava, window, partitioner1, partitioner2, seed * partitioner1.getNumPartitions + idx)
        } else if (samplingMode == SamplingMode.ITEM2VEC_POS2NEG) {
          new Pos2NegGenerator(it.asJava, window, partitioner1, partitioner2, seed * partitioner1.getNumPartitions + idx)
        } else if (samplingMode == SamplingMode.WINDOW) {
          new WindowGenerator(it.asJava, window, partitioner1, partitioner2)
        } else {
          assert(false)
          null
        }
      }, partitioner1.getNumPartitions, false, false).asScala
    })
  }

  override protected def initialize(sent: RDD[Array[Long]]): RDD[ItemData] = {
    var r = sent.flatMap(identity(_)).map(_ -> 1L)
      .reduceByKey(_ + _).filter(_._2 >= minCount)
      .mapPartitions { it =>
        val rnd = new Random()
        it.flatMap { case (w, n) =>
          rnd.setSeed(w.hashCode)
          Iterator(new ItemData(ItemData.TYPE_LEFT, w, n, Optimizer.initEmbedding(dotVectorSize, useBias, rnd)),
            new ItemData(ItemData.TYPE_RIGHT, w, n, Optimizer.initEmbedding(dotVectorSize, useBias, rnd)))
        }
      }

    if (samplingMode == SamplingMode.ITEM2VEC_POS2NEG) {
      r = r.filter(i => i.id > 0 && i.`type` == ItemData.TYPE_LEFT || i.id < 0 && i.`type` == ItemData.TYPE_RIGHT)
    }

    r
  }
}
