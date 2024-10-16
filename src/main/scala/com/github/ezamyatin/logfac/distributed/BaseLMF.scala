package com.github.ezamyatin.logfac.distributed

import com.github.ezamyatin.logfac.local.{ItemData, Optimizer, Opts}
import com.github.ezamyatin.logfac.pair.{LongPair, LongPairMulti, Partitioner}
import com.github.ezamyatin.logfac.pair.generator.BatchedGenerator
import com.github.ezamyatin.logfac.pair.generator.w2v.{Item2VecGenerator, Pos2NegGenerator, SamplingMode}
import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.storage.StorageLevel
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import java.util.Random
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters.{asJavaIteratorConverter, asScalaIteratorConverter}
import scala.util.Try

/**
 * @author ezamyatin
 */
private[distributed] abstract class BaseLMF[T] extends Serializable with Logging {

  protected var dotVectorSize: Int = 100
  protected var negative: Int = 5
  private var numIterations: Int = 1
  private var learningRate: Float = 0.025f
  protected var numThread: Int = 1
  private var numPartitions: Int = 1
  private var pow: Float = 0f
  private var lambdaL: Float = 0f
  private var lambdaR: Float = 0f
  protected var useBias: Boolean = false
  protected var intermediateRDDStorageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK
  protected var checkpointPath: String = _
  protected var checkpointInterval: Int = 0

  protected def gamma: Float = 1f
  protected def implicitPref: Boolean = true

  def setVectorSize(vectorSize: Int): this.type = {
    require(vectorSize > 0,
      s"vector size must be positive but got ${vectorSize}")
    this.dotVectorSize = vectorSize
    this
  }

  def setLearningRate(learningRate: Double): this.type = {
    require(learningRate > 0,
      s"Initial learning rate must be positive but got ${learningRate}")
    this.learningRate = learningRate.toFloat
    this
  }

  def setNumPartitions(numPartitions: Int): this.type = {
    require(numPartitions > 0,
      s"Number of partitions must be positive but got ${numPartitions}")
    this.numPartitions = numPartitions
    this
  }

  def setCheckpointPath(path: String): this.type = {
    this.checkpointPath = path
    this
  }

  def setCheckpointInterval(interval: Int): this.type = {
    this.checkpointInterval = interval
    this
  }

  def setNumIterations(numIterations: Int): this.type = {
    require(numIterations > 0,
      s"Number of iterations must be nonnegative but got ${numIterations}")
    this.numIterations = numIterations
    this
  }

  def setUseBias(useBias: Boolean): this.type = {
    this.useBias = useBias
    this
  }

  def setPow(pow: Double): this.type = {
    require(pow >= 0,
      s"Pow must be positive but got ${pow}")
    this.pow = pow.toFloat
    this
  }

  def setLambdaL(lambdaL: Double): this.type = {
    require(lambdaL >= 0,
      s"Lambda must be positive but got ${lambdaL}")
    this.lambdaL = lambdaL.toFloat
    this
  }

  def setLambdaR(lambdaR: Double): this.type = {
    require(lambdaR >= 0,
      s"Lambda must be positive but got ${lambdaR}")
    this.lambdaR = lambdaR.toFloat
    this
  }

  def setNegative(negative: Int): this.type = {
    require(negative > 0)
    this.negative = negative
    this
  }

  def setNumThread(numThread: Int): this.type = {
    require(numThread >= 0,
      s"Number of threads ${numThread}")
    this.numThread = numThread
    this
  }

  def setIntermediateRDDStorageLevel(storageLevel: StorageLevel): this.type = {
    require(storageLevel != StorageLevel.NONE,
      "SkipGram is not designed to run without persisting intermediate RDDs.")
    this.intermediateRDDStorageLevel = storageLevel
    this
  }

  protected def cacheAndCount[T](rdd: RDD[T]): RDD[T] = {
    val r = rdd.persist(intermediateRDDStorageLevel)
    r.count()
    r
  }

  private def checkpoint(emb: RDD[ItemData],
                 path: String)(implicit sc: SparkContext): RDD[ItemData] = {
    val sqlc = new SQLContext(sc)
    import sqlc.implicits._
    if (emb != null) {
      emb.map(itemData => (itemData.`type`, itemData.id, itemData.cn, itemData.f))
        .toDF("type", "id", "cn", "f")
        .write.mode(SaveMode.Overwrite).parquet(path)
      emb.unpersist()
    }

    cacheAndCount(sqlc.read.parquet(path)
      .as[(Boolean, Long, Long, Array[Float])].rdd
      .map(e => new ItemData(e._1, e._2, e._3, e._4))
    )
  }

  private def listFiles(path: String): Array[String] = {
    val hdfs = FileSystem.get(new Configuration())
    Try(hdfs.listStatus(new Path(path)).map(_.getPath.getName)).getOrElse(Array.empty)
  }

  protected def pairs(sent: RDD[T],
                      partitioner1: Partitioner,
                      partitioner2: Partitioner,
                      seed: Long): RDD[LongPairMulti]

  protected def initialize(sent: RDD[T]): RDD[ItemData]

  protected def doFit(sent: RDD[T]): RDD[ItemData] = {
    val sparkContext = sent.sparkContext

    val latest = if (checkpointPath != null) {
      listFiles(checkpointPath)
        .filter(file => listFiles(checkpointPath + "/" + file).contains("_SUCCESS"))
        .filter(!_.contains("run_params")).filter(_.contains("_"))
        .map(_.split("_").map(_.toInt)).map{case Array(a, b) => (a, b)}
        .sorted.lastOption
    } else {
      None
    }

    latest.foreach(x => println(s"Continue training from epoch = ${x._1}, iteration = ${x._2}"))

    var emb = latest.map(x => checkpoint(null, checkpointPath + "/" + x._1 + "_" + x._2)(sparkContext))
      .getOrElse{cacheAndCount(initialize(sent))}

    val (startEpoch, startIter) = latest.getOrElse((0, 0))
    var checkpointIter = startEpoch * numPartitions + startIter
    val cached = ArrayBuffer.empty[RDD[ItemData]]
    val partitionTable = sparkContext.broadcast(Partitioner.createPartitionTable(numPartitions, new Random(0)))

    (startEpoch until numIterations).foreach {curEpoch =>

      val partitioner1 = new Partitioner {
        override def getPartition(item: Long): Int = Partitioner.hash(item, curEpoch, numPartitions)
        override def getNumPartitions: Int = numPartitions
      }

      ((if (curEpoch == startEpoch) startIter else 0) until numPartitions).foreach { pI =>
        val partitioner2 = new Partitioner {
          override def getPartition(item: Long): Int = {
            val bucket = Partitioner.hash(item, curEpoch, partitionTable.value.length)
            partitionTable.value.apply(bucket).apply(pI)
          }
          override def getNumPartitions: Int = numPartitions
        }

        val partitionerKey = new HashPartitioner(numPartitions) {
          override def getPartition(key: Any): Int = key.asInstanceOf[Int]
        }

        val embLR = emb
          .keyBy(i => if (i.`type` == ItemData.TYPE_LEFT) partitioner1.getPartition(i.id) else partitioner2.getPartition(i.id))
          .partitionBy(partitionerKey).values

        val cur = pairs(sent, partitioner1, partitioner2, (1L * curEpoch * numPartitions + pI) * numPartitions)
            .map(e => e.part -> e).partitionBy(partitionerKey).values

        emb = cur.zipPartitions(embLR) { case (sIt, eItLR) =>
          val opts = if (implicitPref) {
            Opts.`implicit`(dotVectorSize, useBias, negative, pow, learningRate, lambdaL, lambdaR, gamma, false)
          } else {
            Opts.explicit(dotVectorSize, useBias, learningRate, lambdaL, lambdaR, false)
          }

          var time = System.currentTimeMillis()
          println("loading...")
          val sg = new Optimizer(opts, eItLR.asJava)
          println(s"loaded in ${(System.currentTimeMillis() - time) / 1000} seconds. optimizing...")
          time = System.currentTimeMillis()
          sg.optimize(sIt.asJava, numThread)

          if (opts.verbose) {
            println("LOSS: " + sg.loss.doubleValue() + "\t" + sg.lossReg.doubleValue())
          }
          println(s"optimized in ${(System.currentTimeMillis() - time) / 1000} seconds. flushing...")

          sg.flush().asScala
        }.persist(intermediateRDDStorageLevel)

        cached += emb

        if (checkpointInterval > 0 && (checkpointIter + 1) % checkpointInterval == 0) {
          emb = checkpoint(emb, checkpointPath + "/" + curEpoch + "_" + (pI + 1))(sparkContext)
          cached.foreach(_.unpersist())
          cached.clear()
        }
        checkpointIter += 1
      }
    }

    emb
  }

  def fit(dataset: DataFrame): RDD[ItemData]
}
