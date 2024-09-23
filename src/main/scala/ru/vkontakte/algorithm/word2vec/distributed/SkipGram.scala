package ru.vkontakte.algorithm.word2vec.distributed

import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.storage.StorageLevel
import ru.vkontakte.algorithm.word2vec.distributed.SkipGram.ItemID
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import ru.vkontakte.algorithm.word2vec.local.{ItemData, ParItr, SkipGramLocal, SkipGramOpts}
import ru.vkontakte.algorithm.word2vec.pair.{LongPairMulti, SamplingMode, SkipGramPartitioner}
import ru.vkontakte.algorithm.word2vec.pair.generator.{BatchedGenerator, Pos2NegPairGenerator, SampleGenerator}

import java.util.Random
import java.util.function.Consumer
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters.{asJavaIteratorConverter, asScalaIteratorConverter}
import scala.util.Try

object SkipGram {
  type ItemID = Long
}

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

  def checkpoint(emb: RDD[(ItemID, (Long, Array[Float], Array[Float]))],
                 path: String)(implicit sc: SparkContext): RDD[(ItemID, (Long, Array[Float], Array[Float]))] = {
    val sqlc = new SQLContext(sc)
    import sqlc.implicits._
    if (emb != null) {
      emb.toDF("user_id", "emb").write.mode(SaveMode.Overwrite).parquet(path)
      emb.unpersist()
    }
    cacheAndCount(sqlc.read.parquet(path)
      .as[(ItemID, (Long, Array[Float], Array[Float]))]
      .rdd
    )
  }

  def fit(dataset: RDD[Array[ItemID]]): RDD[(ItemID, (Long, Array[Float], Array[Float]))] = {
    assert(!((checkpointInterval > 0) ^ (checkpointPath != null)))

    val sc = dataset.context

    val numExecutors = sc.getConf.get("spark.executor.instances").toInt
    val numCores = sc.getConf.get("spark.executor.cores").toInt
    val sent = cacheAndCount(dataset.repartition(numExecutors * numCores / numThread))

    try {
      doFit(sent)
    } finally {
      sent.unpersist()
    }
  }

  def listFiles(path: String): Array[String] = {
    val hdfs = FileSystem.get(new Configuration())
    Try(hdfs.listStatus(new Path(path)).map(_.getPath.getName)).getOrElse(Array.empty)
  }

  private def doFit(sent: RDD[Array[ItemID]]): RDD[(ItemID, (Long, Array[Float], Array[Float]))] = {
    import SkipGram._

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

    var emb = latest.map(x => checkpoint(null, checkpointPath + "/" + x._1 + "_" + x._2)(sent.sparkContext))
      .getOrElse{cacheAndCount(
        sent
          .flatMap(identity(_)).map(_ -> 1L)
          .reduceByKey(_ + _).filter(_._2 >= minCount)
          .mapPartitions{it =>
            val rnd = new Random()
            it.map{case (w, n) =>
              rnd.setSeed(w.hashCode)
              w -> (n, SkipGramLocal.initEmbedding(dotVectorSize, useBias, rnd),
                SkipGramLocal.initEmbedding(dotVectorSize, useBias, rnd))
            }
      })}

    var checkpointIter = 0
    val (startEpoch, startIter) = latest.getOrElse((0, 0))
    val cached = ArrayBuffer.empty[RDD[(ItemID, (Long, Array[Float], Array[Float]))]]
    val partitionTable = sent.sparkContext.broadcast(SkipGramPartitioner.createPartitionTable(numPartitions, new Random(0)))

    (startEpoch until numIterations).foreach {curEpoch =>

      val partitioner1 = new SkipGramPartitioner {
        override def getPartition(item: ItemID): Int = SkipGramPartitioner.hash(item, curEpoch, numPartitions)
        override def getNumPartitions: Int = numPartitions
      }

      ((if (curEpoch == startEpoch) startIter else 0) until numPartitions).foreach { pI =>
        val progress = (1.0 * curEpoch.toDouble * numPartitions + pI) / (numIterations * numPartitions)
        val curLearningRate = minLearningRate.fold(learningRate)(e => Math.exp(Math.log(learningRate) - (Math.log(learningRate) - Math.log(e)) * progress))
        val partitioner2 = new SkipGramPartitioner {
          override def getPartition(item: ItemID): Int = {
            val bucket = SkipGramPartitioner.hash(item, curEpoch, partitionTable.value.length)
            partitionTable.value.apply(bucket).apply(pI)
          }
          override def getNumPartitions: Int = numPartitions
        }

        val partitionerKey = new HashPartitioner(numPartitions) {
          override def getPartition(key: Any): Int = key.asInstanceOf[Int]
        }

        val embLR = emb.flatMap{x =>
          Iterator(partitioner1.getPartition(x._1) -> new ItemData(ItemData.TYPE_LEFT, x._1, x._2._1, x._2._2),
            partitioner2.getPartition(x._1) -> new ItemData(ItemData.TYPE_RIGHT, x._1, x._2._1, x._2._3))
        }.partitionBy(partitionerKey).values

        val cur = sent.mapPartitionsWithIndex({case (idx, it) =>
          val seed = (1L * curEpoch * numPartitions + pI) * numPartitions + idx

          val pairGenerator = new BatchedGenerator({
            if (samplingMode == SamplingMode.SAMPLE) {
              new SampleGenerator(window, samplingMode, partitioner1, partitioner2, seed)
            } else if (samplingMode == SamplingMode.SAMPLE_POS2NEG) {
              new Pos2NegPairGenerator(window, samplingMode, partitioner1, partitioner2, seed)
            } else if (samplingMode == SamplingMode.WINDOW) {
              assert(false)
              null
            } else {
              assert(false)
              null
            }
          })

          it.flatMap(it => pairGenerator.generate(it).asScala) ++ pairGenerator.flush().asScala
        }).map(e => e.part -> e).partitionBy(partitionerKey).values

        val newEmb = (cur.zipPartitions(embLR) { case (sIt, eItLR) =>

          val sg = new SkipGramLocal(new SkipGramOpts(dotVectorSize, useBias, negative, window,
            pow, curLearningRate, lambda, samplingMode), eItLR.asJava)

          sg.optimize(sIt.asJava, numThread)

          println("LOSS: " + sg.loss.doubleValue() / sg.lossn.longValue() + " (" + sg.loss.doubleValue() + " / " + sg.lossn.longValue() + ")")

          sg.flush().asScala.map{itemData =>
            if (itemData.`type` == ItemData.TYPE_LEFT) {
              itemData.id -> (itemData.cn, itemData.f, null.asInstanceOf[Array[Float]])
            } else {
              itemData.id -> (itemData.cn, null.asInstanceOf[Array[Float]], itemData.f)
            }
          }
        })

        emb = newEmb
          .reduceByKey{case (x, y) => (x._1 + y._1, if (x._2 != null) x._2 else y._2, if (x._3 == null) y._3 else x._3)}
          .persist(intermediateRDDStorageLevel)
        cached += emb

        if (checkpointInterval > 0 && (checkpointIter + 1) % checkpointInterval == 0) {
          emb = checkpoint(emb, checkpointPath + "/" + curEpoch + "_" + (pI + 1))(sent.sparkContext)
          cached.foreach(_.unpersist())
          cached.clear()
        }
        checkpointIter += 1
      }
    }

    emb
  }
}
