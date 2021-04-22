package com.skonuniverse.flink.source

import com.skonuniverse.flink.datatype.Message
import com.skonuniverse.flink.source.MessageCheckpointSerializer.serializeV1
import com.skonuniverse.flink.source.util.{IteratorSourceEnumerator, IteratorSourceReader, IteratorSourceSplit}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.connector.source.{Boundedness, Source, SourceReader, SourceReaderContext, SplitEnumerator, SplitEnumeratorContext}
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.core.io.SimpleVersionedSerializer
import org.apache.flink.core.memory.{DataInputDeserializer, DataOutputSerializer}
import org.apache.flink.shaded.guava18.com.google.common.hash.Hashing

import java.io.IOException
import collection.JavaConverters._
import java.sql.Timestamp
import scala.util.Random

class MessageSource(codes: Seq[Int]) extends Source[Message, MessageSplit, java.util.Collection[MessageSplit]] with ResultTypeQueryable[Message] {
  private val uniqueCodes = codes.distinct
  require(uniqueCodes.nonEmpty, "codes should not be empty")

  override def getBoundedness: Boundedness = Boundedness.CONTINUOUS_UNBOUNDED

  override def createReader(readerContext: SourceReaderContext): SourceReader[Message, MessageSplit] = {
    new IteratorSourceReader[Message, MessageIterator, MessageSplit](readerContext)
  }

  override def createEnumerator(enumContext: SplitEnumeratorContext[MessageSplit]): SplitEnumerator[MessageSplit, java.util.Collection[MessageSplit]] = {
    val messageIterators = new MessageIterator(codes)
        .split(enumContext.currentParallelism())
    val splits = messageIterators
        .map(iterator => new MessageSplit(Hashing.sha256().hashInt(Random.nextInt()).toString, iterator.asInstanceOf[MessageIterator].getCodes))

    new IteratorSourceEnumerator[MessageSplit](enumContext, splits)
  }

  override def restoreEnumerator(enumContext: SplitEnumeratorContext[MessageSplit], checkpoint: java.util.Collection[MessageSplit]): SplitEnumerator[MessageSplit, java.util.Collection[MessageSplit]] = {
    new IteratorSourceEnumerator[MessageSplit](enumContext, checkpoint.asScala)
  }

  override def getSplitSerializer: SimpleVersionedSerializer[MessageSplit] = new MessageSplitSerializer

  override def getEnumeratorCheckpointSerializer: SimpleVersionedSerializer[java.util.Collection[MessageSplit]] = new MessageCheckpointSerializer

  override def getProducedType: TypeInformation[Message] = TypeInformation.of(classOf[Message])
}

class MessageSplitSerializer extends SimpleVersionedSerializer[MessageSplit] {
  private final val version = 1

  override def getVersion: Int = version

  override def serialize(split: MessageSplit): Array[Byte] = {
    val serializer = new DataOutputSerializer(MessageCheckpointSerializer.getMemorySize(split))
    MessageCheckpointSerializer.serializeV1(serializer, split)
    serializer.getCopyOfBuffer
  }

  @throws(classOf[IOException])
  override def deserialize(version: Int, serialized: Array[Byte]): MessageSplit = {
    if (version != this.version) {
      throw new IOException("Unknown version of split serializer: " + version)
    }

    val deserializer = new DataInputDeserializer(serialized)
    MessageCheckpointSerializer.deserializeV1(deserializer)
  }
}

class MessageCheckpointSerializer extends SimpleVersionedSerializer[java.util.Collection[MessageSplit]] {
  private final val version = 1

  override def getVersion: Int = version

  override def serialize(splits: java.util.Collection[MessageSplit]): Array[Byte] = {
    val splitIterator = splits.asScala
    val memorySize = splitIterator.map(split => MessageCheckpointSerializer.getMemorySize(split)).sum
    val serializer = new DataOutputSerializer(4 + memorySize)

    serializer.writeInt(splits.size)
    splitIterator.foreach(split => serializeV1(serializer, split))
    serializer.getCopyOfBuffer
  }

  @throws(classOf[IOException])
  override def deserialize(version: Int, serialized: Array[Byte]): java.util.Collection[MessageSplit] = {
    if (version != this.version) {
      throw new IOException("Unknown version of checkpoint serializer: " + version)
    }

    val deserializer = new DataInputDeserializer(serialized)
    val size = deserializer.readInt()
    (0 until size).map(_ => MessageCheckpointSerializer.deserializeV1(deserializer)).asJavaCollection
  }
}

object MessageCheckpointSerializer {
  def getMemorySize(split: MessageSplit): Int = {
    split.splitId().length + (split.getCodes.size + 1) * 4
  }

  def serializeV1(serializer: DataOutputSerializer, split: MessageSplit): Unit = {
    serializer.writeUTF(split.splitId())
    serializer.writeInt(split.getCodes.size)
    split.getCodes.foreach(code => serializer.writeInt(code))
  }

  def deserializeV1(deserializer: DataInputDeserializer): MessageSplit = {
    val splitId = deserializer.readUTF()
    val size = deserializer.readInt()
    val codes = (0 until size).map(_ => deserializer.readInt)

    new MessageSplit(splitId, codes)
  }
}

sealed class MessageSplit(splitId: String, codes: Seq[Int]) extends IteratorSourceSplit[Message, MessageIterator] {
  def getCodes: Seq[Int] = codes

  override def getIterator: MessageIterator = new MessageIterator(codes)

  override def getUpdatedSplitForIterator(iterator: MessageIterator): IteratorSourceSplit[Message, MessageIterator] = {
    new MessageSplit(splitId, iterator.getCodes)
  }

  override def splitId(): String = splitId
}

sealed class MessageIterator(codes: Seq[Int]) extends SplittableIterator[Message] {
  def this(code: Int) = this(Seq(code))

  def getCodes: Seq[Int] = codes

  override def split(numPartitions: Int): Seq[Iterator[Message]] = {
    if (numPartitions <= 0) {
      throw new IllegalArgumentException("The number of partitions must be smaller or equal than number of codes")
    }
    val availableNumPartitions = Math.min(numPartitions, getMaximumNumberOfSplits)

    if (availableNumPartitions == 1) {
      Seq(new MessageIterator(codes))
    } else {
      val codesPerSplit = Math.max(1, codes.size / (availableNumPartitions + 1) + 1)
      if (codesPerSplit == 1) {
        val a = Random.shuffle(codes).take(availableNumPartitions)
            .map(code => new MessageIterator(code))
        a
      } else {
        val messageIterators = (0 until availableNumPartitions - 1).map(i => {
          new MessageIterator(codes.slice(i * codesPerSplit, (i + 1) * codesPerSplit))
        })
        messageIterators :+ new MessageIterator(codes.slice((availableNumPartitions - 1) * codesPerSplit, codes.size))
      }
    }
  }

  override def getMaximumNumberOfSplits: Int = codes.size

  override def hasNext: Boolean = true

  override def next(): Message = Message(eventTime = new Timestamp(System.currentTimeMillis()),
                                         code = codes.apply(Random.nextInt(codes.size)),
                                         value = Random.nextDouble())
}