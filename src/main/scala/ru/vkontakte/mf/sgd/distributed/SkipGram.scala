package ru.vkontakte.mf.sgd.distributed

import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.storage.StorageLevel
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import ru.vkontakte.mf.sgd.local.{ItemData, ParItr, Optimizer, Opts}
import ru.vkontakte.mf.sgd.pair.{LongPair, LongPairMulti, SkipGramPartitioner}
import ru.vkontakte.mf.sgd.pair.generator.BatchedGenerator
import ru.vkontakte.mf.sgd.pair.generator.w2v.{Pos2NegPairGenerator, SampleGenerator, SamplingMode}

import java.util.Random
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters.{asJavaIteratorConverter, asScalaIteratorConverter}
import scala.util.Try

/**
 * @author ezamyatin
 * */
class SkipGram extends Serializable with Logging {

  private var dotVectorSize: Int = 100
  private var window: Int = 5
  private var samplingMode: SamplingMode = SamplingMode.SAMPLE
  private var negative: Int = 5
  private var numIterations: Int = 1
  private var learningRate: Double = 0.025
  private var minLearningRate: Option[Double] = None
  private var minCount: Int = 1
  private var numThread: Int = 1
  private var numPartitions: Int = 1
  private var pow: Double = 0
  private var lambda: Double = 0
  private var useBias: Boolean = false
  private var intermediateRDDStorageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK
  private var checkpointPath: String = _
  private var checkpointInterval: Int = 0

  def setVectorSize(vectorSize: Int): this.type = {
    require(vectorSize > 0,
      s"vector size must be positive but got ${vectorSize}")
    this.dotVectorSize = vectorSize
    this
  }

  def setLearningRate(learningRate: Double): this.type = {
    require(learningRate > 0,
      s"Initial learning rate must be positive but got ${learningRate}")
    this.learningRate = learningRate
    this
  }

  def setMinLearningRate(minLearningRate: Option[Double]): this.type = {
    require(minLearningRate.forall(_ > 0),
      s"Initial learning rate must be positive but got ${minLearningRate}")
    this.minLearningRate = minLearningRate
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
    require(numIterations >= 0,
      s"Number of iterations must be nonnegative but got ${numIterations}")
    this.numIterations = numIterations
    this
  }

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

  def setUseBias(useBias: Boolean): this.type = {
    this.useBias = useBias
    this
  }

  def setPow(pow: Double): this.type = {
    require(pow >= 0,
      s"Pow must be positive but got ${pow}")
    this.pow = pow
    this
  }

  def setLambda(lambda: Double): this.type = {
    require(lambda >= 0,
      s"Lambda must be positive but got ${lambda}")
    this.lambda = lambda
    this
  }

  def setMinCount(minCount: Int): this.type = {
    require(minCount >= 0,
      s"Minimum number of times must be nonnegative but got ${minCount}")
    this.minCount = minCount
    this
  }

  def setNegative(negative: Int): this.type = {
    require(negative >= 0,
      s"Number of negative samples ${negative}")
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

  private def cacheAndCount[T](rdd: RDD[T]): RDD[T] = {
    val r = rdd.persist(intermediateRDDStorageLevel)
    r.count()
    r
  }

  def checkpoint(emb: RDD[ItemData],
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

  def fitW2V(dataset: RDD[Array[Long]]): RDD[ItemData] = {
    assert(!((checkpointInterval > 0) ^ (checkpointPath != null)))

    val sc = dataset.context

    val numExecutors = sc.getConf.get("spark.executor.instances").toInt
    val numCores = sc.getConf.get("spark.executor.cores").toInt
    val sent = cacheAndCount(dataset.repartition(numExecutors * numCores / numThread))

    try {
      doFit(Left(sent))
    } finally {
      sent.unpersist()
    }
  }

  def fitLMF(dataset: RDD[(Long, Long, Float)]): RDD[ItemData] = {
    assert(!((checkpointInterval > 0) ^ (checkpointPath != null)))

    val sc = dataset.context

    val numExecutors = sc.getConf.get("spark.executor.instances").toInt
    val numCores = sc.getConf.get("spark.executor.cores").toInt
    val sent = cacheAndCount(dataset.repartition(numExecutors * numCores / numThread))

    try {
      doFit(Right(sent))
    } finally {
      sent.unpersist()
    }
  }

  def listFiles(path: String): Array[String] = {
    val hdfs = FileSystem.get(new Configuration())
    Try(hdfs.listStatus(new Path(path)).map(_.getPath.getName)).getOrElse(Array.empty)
  }

  private def pairs(sent: Either[RDD[Array[Long]], RDD[(Long, Long, Float)]],
                    curEpoch: Int, pI: Int,
                    partitioner1: SkipGramPartitioner,
                    partitioner2: SkipGramPartitioner): RDD[LongPairMulti] = {
    sent.fold ({_.mapPartitionsWithIndex({ case (idx, it) =>
        val seed = (1L * curEpoch * numPartitions + pI) * numPartitions + idx
        new BatchedGenerator({
          if (samplingMode == SamplingMode.SAMPLE) {
            new SampleGenerator(it.asJava, window, samplingMode, partitioner1, partitioner2, seed)
          } else if (samplingMode == SamplingMode.SAMPLE_POS2NEG) {
            new Pos2NegPairGenerator(it.asJava, window, samplingMode, partitioner1, partitioner2, seed)
          } else if (samplingMode == SamplingMode.WINDOW) {
            assert(false)
            null
          } else {
            assert(false)
            null
          }
        }, partitioner1.getNumPartitions, false).asScala
      })
    }, _.mapPartitions(it => new BatchedGenerator(it
      .filter(e => partitioner1.getPartition(e._1) == partitioner2.getPartition(e._2))
      .map(e => new LongPair(partitioner1.getPartition(e._1), e._1, e._2, e._3))
      .asJava, partitioner1.getNumPartitions, true).asScala))
  }

  private def doFit(sent: Either[RDD[Array[Long]], RDD[(Long, Long, Float)]]): RDD[ItemData] = {
    val sparkContext = sent.fold(_.sparkContext, _.sparkContext)

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
      .getOrElse{cacheAndCount(
        sent.fold(_.flatMap(identity(_)).map(_ -> 1L)
          .reduceByKey(_ + _).filter(_._2 >= minCount)
          .mapPartitions{it =>
            val rnd = new Random()
            it.flatMap {case (w, n) =>
              rnd.setSeed(w.hashCode)
              Iterator(new ItemData(ItemData.TYPE_LEFT, w, n, Optimizer.initEmbedding(dotVectorSize, useBias, rnd)),
                new ItemData(ItemData.TYPE_RIGHT, w, n, Optimizer.initEmbedding(dotVectorSize, useBias, rnd)))
            }
      }, _.flatMap(e => Seq((ItemData.TYPE_LEFT, e._1) -> 1, (ItemData.TYPE_RIGHT, e._2) -> 1))
          .reduceByKey(_ + _).filter(_._2 >= minCount)
          .mapPartitions { it =>
            val rnd = new Random()
            it.map { case ((t, w), n) =>
              rnd.setSeed(w.hashCode)
              new ItemData(t, w, n, Optimizer.initEmbedding(dotVectorSize, useBias, rnd))
            }
          }
      ))}

    if (samplingMode == SamplingMode.SAMPLE_POS2NEG) {
      emb = emb.filter(i => i.id > 0 && i.`type` == ItemData.TYPE_LEFT || i.id < 0 && i.`type` == ItemData.TYPE_RIGHT)
    }

    var checkpointIter = 0
    val (startEpoch, startIter) = latest.getOrElse((0, 0))
    val cached = ArrayBuffer.empty[RDD[ItemData]]
    val partitionTable = sparkContext.broadcast(SkipGramPartitioner.createPartitionTable(numPartitions, new Random(0)))

    (startEpoch until numIterations).foreach {curEpoch =>

      val partitioner1 = new SkipGramPartitioner {
        override def getPartition(item: Long): Int = SkipGramPartitioner.hash(item, curEpoch, numPartitions)
        override def getNumPartitions: Int = numPartitions
      }

      ((if (curEpoch == startEpoch) startIter else 0) until numPartitions).foreach { pI =>
        val progress = (1.0 * curEpoch.toDouble * numPartitions + pI) / (numIterations * numPartitions)
        val curLearningRate = minLearningRate.fold(learningRate)(e => Math.exp(Math.log(learningRate) - (Math.log(learningRate) - Math.log(e)) * progress))
        val partitioner2 = new SkipGramPartitioner {
          override def getPartition(item: Long): Int = {
            val bucket = SkipGramPartitioner.hash(item, curEpoch, partitionTable.value.length)
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

        val cur = pairs(sent, curEpoch, pI, partitioner1, partitioner2)
          .map(e => e.part -> e).partitionBy(partitionerKey).values

        emb = cur.zipPartitions(embLR) { case (sIt, eItLR) =>
          val sg = new Optimizer(new Opts(dotVectorSize, useBias, negative, window,
            pow, curLearningRate, lambda), eItLR.asJava)

          sg.optimize(sIt.asJava, numThread)

          println("LOSS: " + sg.loss.doubleValue() / sg.lossn.longValue() + " (" + sg.loss.doubleValue() + " / " + sg.lossn.longValue() + ")")

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
}
