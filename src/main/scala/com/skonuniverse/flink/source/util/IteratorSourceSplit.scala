package com.skonuniverse.flink.source.util

import org.apache.flink.api.connector.source.SourceSplit

// org.apache.flink.api.connector.source.lib.util.IteratorSourceSplit
trait IteratorSourceSplit[E, IterT <: Iterator[E]] extends SourceSplit {
  def getIterator: IterT
  def getUpdatedSplitForIterator(iterator: IterT): IteratorSourceSplit[E, IterT]
}
