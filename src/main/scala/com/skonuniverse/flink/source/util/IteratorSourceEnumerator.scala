package com.skonuniverse.flink.source.util

import org.apache.flink.api.connector.source.{SplitEnumerator, SplitEnumeratorContext}

import scala.collection.JavaConverters._
import scala.collection.mutable

// org.apache.flink.api.connector.source.lib.util.IteratorSourceEnumerator
class IteratorSourceEnumerator[SplitT <: IteratorSourceSplit[_, _]](context: SplitEnumeratorContext[SplitT], splits: Iterable[SplitT])
    extends SplitEnumerator[SplitT, java.util.Collection[SplitT]] {
  private val remaingSplits: mutable.Queue[SplitT] = {
    val queue = new mutable.Queue[SplitT]
    splits.foreach(split => queue.enqueue(split))
    queue
  }

  override def start(): Unit = {}

  override def close(): Unit = {}

  override def handleSplitRequest(subtaskId: Int, requesterHostname: String): Unit = {
    try {
      val nextSplit = remaingSplits.dequeue()
      context.assignSplit(nextSplit, subtaskId)
    } catch {
      case _: NoSuchElementException =>
        context.signalNoMoreSplits(subtaskId)
    }

  }

  override def addSplitsBack(splits: java.util.List[SplitT], subtaskId: Int): Unit = {
    addSplitsBack(splits.asScala)
  }

  def addSplitsBack(splits: Seq[SplitT]): Unit = {
    splits.foreach(split => remaingSplits.enqueue(split))
  }

  override def addReader(subtaskId: Int): Unit = {}

  override def snapshotState(): java.util.Collection[SplitT] = remaingSplits.asJavaCollection

}
