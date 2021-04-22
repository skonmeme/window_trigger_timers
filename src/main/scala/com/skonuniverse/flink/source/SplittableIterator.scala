package com.skonuniverse.flink.source

@SerialVersionUID(1L)
trait SplittableIterator[T] extends Iterator[T] with Serializable {
  def split(numPartitions: Int): Seq[Iterator[T]]

  def getSplit(num: Int, numPartitions: Int): Iterator[T] = {
    if (numPartitions < 1 || num < 0 || num >= numPartitions) throw new IllegalArgumentException
    split(numPartitions).apply(num)
  }

  def getMaximumNumberOfSplits: Int
}
