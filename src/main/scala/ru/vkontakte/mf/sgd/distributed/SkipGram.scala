package ru.vkontakte.mf.sgd.distributed

import org.apache.spark.rdd.RDD
import ru.vkontakte.mf.sgd.local.ItemData
import ru.vkontakte.mf.sgd.pair.generator.w2v.SamplingMode

/**
 * @author ezamyatin
 * */
class SkipGram extends LMF {

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

  override def setMinUserCount(minCount: Int): this.type = {
    throw new UnsupportedOperationException("minUserCount is not allowed in SkipGram mode")
    this
  }

  def fit(dataset: RDD[Array[Long]]): RDD[ItemData] = {
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
}
