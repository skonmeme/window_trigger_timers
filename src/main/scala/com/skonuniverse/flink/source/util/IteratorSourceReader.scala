package com.skonuniverse.flink.source.util

import org.apache.flink.api.connector.source.{ReaderOutput, SourceReader, SourceReaderContext}
import org.apache.flink.core.io.InputStatus
import org.apache.flink.util.Preconditions.checkState

import java.util
import java.util.Collections
import java.util.concurrent.CompletableFuture
import scala.collection.JavaConverters._
import scala.collection.mutable

// org.apache.flink.api.connector.source.lib.util.IteratorSourceReader
class IteratorSourceReader[E, IterT <: Iterator[E], SplitT <: IteratorSourceSplit[E, IterT]](context: SourceReaderContext)
    extends SourceReader[E, SplitT] {
  private val availability = new CompletableFuture[Void]()

  private var remainingSplits: mutable.Queue[SplitT] = _
  private var iterator: IterT = _
  private var currentSplit: SplitT = _

  override def start(): Unit = {
    if (remainingSplits == null) context.sendSplitRequest()
  }

  override def pollNext(output: ReaderOutput[E]): InputStatus = {
    if (iterator != null && iterator.hasNext) {
      output.collect(iterator.next())
      InputStatus.MORE_AVAILABLE
    } else if (remainingSplits == null) {
      InputStatus.NOTHING_AVAILABLE
    } else {
      try {
        currentSplit = remainingSplits.dequeue()
        iterator = currentSplit.getIterator
        pollNext(output)
      } catch {
        case _: NoSuchElementException =>
          InputStatus.END_OF_INPUT
      }
    }
  }

  override def isAvailable: CompletableFuture[Void] = availability

  override def addSplits(splits: util.List[SplitT]): Unit = {
    addSplits(splits.asScala)
  }

  def addSplits(splits: Seq[SplitT]): Unit = {
    checkState(remainingSplits == null, "Cannot accept more than one split assignment".asInstanceOf[Object])
    remainingSplits = new mutable.Queue[SplitT]
    splits.foreach(split => remainingSplits.enqueue(split))
    availability.complete(null)
  }

  override def notifyNoMoreSplits(): Unit = {
    checkState(remainingSplits == null, "Unexpected response, requested more than one split".asInstanceOf[Object])
    remainingSplits = new mutable.Queue[SplitT]()
  }

  override def snapshotState(checkpointId: Long): util.List[SplitT] = {
    if (remainingSplits == null) {
      Collections.emptyList()
    } else {
      val allSplits = {
        if (iterator != null && iterator.hasNext) {
          val inProgressSplit = currentSplit.getUpdatedSplitForIterator(iterator).asInstanceOf[SplitT]
          inProgressSplit +: remainingSplits.toList
        } else {
          remainingSplits.toList
        }
      }
      allSplits.asJava
    }
  }

  override def close(): Unit = {}
}
