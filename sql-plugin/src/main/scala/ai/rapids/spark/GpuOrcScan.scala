/*
 * Copyright (c) 2019, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ai.rapids.spark

import ai.rapids.cudf.{HostMemoryBuffer, ORCOptions, Table}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress.CompressionCodecFactory

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{PartitionedFile, PartitioningAwareFileIndex}
import org.apache.spark.sql.execution.datasources.v2.orc.OrcScan
import org.apache.spark.sql.execution.datasources.v2.FilePartitionReaderFactory
import org.apache.spark.sql.execution.QueryExecutionException
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.sources.v2.reader.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.vectorized.ColumnarBatch

class GpuOrcScan(
    sparkSession: SparkSession,
    hadoopConf: Configuration,
    fileIndex: PartitioningAwareFileIndex,
    dataSchema: StructType,
    readDataSchema: StructType,
    readPartitionSchema: StructType,
    options: CaseInsensitiveStringMap,
    pushedFilters: Array[Filter])
  extends OrcScan(sparkSession, hadoopConf, fileIndex, dataSchema,
    readDataSchema, readPartitionSchema, options, pushedFilters) with GpuScan {

  // Splitting an individual ORC file is currently not supported.
  override def isSplitable(path: Path): Boolean = false

  override def createReaderFactory(): PartitionReaderFactory = {
    val broadcastedConf = sparkSession.sparkContext.broadcast(
      new GpuSerializableConfiguration(hadoopConf))
    GpuOrcPartitionReaderFactory(sparkSession.sessionState.conf, broadcastedConf,
      dataSchema, readDataSchema, readPartitionSchema)
  }

  // TODO need a common base for these...
  override def equals(other: Any): Boolean = {
    if (!super.equals(other)) {
      return false
    }
    other.isInstanceOf[GpuOrcScan]
  }

  override def hashCode(): Int = super.hashCode()
}

object GpuOrcScan {
  def assertCanSupport(scan: OrcScan): Unit = {
    val schema = StructType(scan.readDataSchema ++ scan.readPartitionSchema)
    for (field <- schema) {
      if (!GpuColumnVector.isSupportedType(field.dataType)) {
        throw new CannotReplaceException(s"GpuOrcScan does not support fields of type ${field.dataType}")
      }
    }
  }
}

case class GpuOrcPartitionReaderFactory(
    sqlConf: SQLConf,
    broadcastedConf: Broadcast[GpuSerializableConfiguration],
    dataSchema: StructType,
    readDataSchema: StructType,
    partitionSchema: StructType) extends FilePartitionReaderFactory {

  override def supportColumnarReads(partition: InputPartition): Boolean = true

  override def buildReader(partitionedFile: PartitionedFile): PartitionReader[InternalRow] = {
    throw new IllegalStateException("GPU column parser called to read rows")
  }

  override def buildColumnarReader(partitionedFile: PartitionedFile): PartitionReader[ColumnarBatch] = {
    val conf = broadcastedConf.value.value
    val reader = new GpuOrcPartitionReader(conf, partitionedFile, dataSchema, readDataSchema)
    ColumnarPartitionReaderWithPartitionValues.newReader(partitionedFile, reader, partitionSchema)
  }
}

class GpuOrcPartitionReader(
    conf: Configuration,
    partFile: PartitionedFile,
    dataSchema: StructType,
    readDataSchema: StructType) extends PartitionReader[ColumnarBatch] with Logging {
  var batch: Option[ColumnarBatch] = None

  override def next(): Boolean = {
    if (batch.isDefined) {
      batch.foreach(_.close())
      batch = None
    } else {
      val table = readToTable()
      try {
        batch = table.map(GpuColumnVector.from)
      } finally {
        table.foreach(_.close())
      }
    }
    batch.isDefined
  }

  override def get(): ColumnarBatch = batch.getOrElse(throw new NoSuchElementException)

  override def close(): Unit = {
    batch.foreach(_.close())
    batch = None
  }

  /**
    * Grows a host buffer, returning a new buffer and closing the original
    * after copying the data into the new buffer.
    *
    * @param original the original host memory buffer
    */
  private def growHostBuffer(original: HostMemoryBuffer, needed: Long): HostMemoryBuffer = {
    val newSize = Math.max(original.getLength * 2, needed)
    val result = HostMemoryBuffer.allocate(newSize)
    try {
      result.copyFromHostBuffer(0, original, 0, original.getLength)
      original.close()
    } catch {
      case e: Throwable =>
        result.close()
        throw e
    }
    result
  }

  private def readPartFile(): (HostMemoryBuffer, Long) = {
    val rawPath = new Path(partFile.filePath)
    val fs = rawPath.getFileSystem(conf)
    val path = fs.makeQualified(rawPath)
    val fileSize = fs.getFileStatus(path).getLen
    var succeeded = false
    var hmb = HostMemoryBuffer.allocate(fileSize)
    var totalBytesRead: Long = 0L
    try {
      val buffer = new Array[Byte](1024 * 16)
      val codecFactory = new CompressionCodecFactory(conf)
      val codec = codecFactory.getCodec(path)
      val rawInput = fs.open(path)
      val in = if (codec != null) codec.createInputStream(rawInput) else rawInput
      try {
        var numBytes = in.read(buffer)
        while (numBytes >= 0) {
          if (totalBytesRead + numBytes > hmb.getLength) {
            hmb = growHostBuffer(hmb, totalBytesRead + numBytes)
          }
          hmb.setBytes(totalBytesRead, buffer, 0, numBytes)
          totalBytesRead += numBytes
          numBytes = in.read(buffer)
        }
      } finally {
        in.close()
      }
      succeeded = true
    } finally {
      if (!succeeded) {
        hmb.close()
      }
    }
    (hmb, totalBytesRead)
  }

  private def readToTable(): Option[Table] = {
    val (dataBuffer, dataSize) = readPartFile()
    try {
      if (dataSize == 0) {
        None
      } else {
        val parseOpts = ORCOptions.builder().includeColumn(readDataSchema.fieldNames: _*).build()
        val table = Table.readORC(parseOpts, dataBuffer, 0, dataSize)
        val numColumns = table.getNumberOfColumns
        if (readDataSchema.length != numColumns) {
          table.close()
          throw new QueryExecutionException(s"Expected ${readDataSchema.length} columns " +
              s"but read ${table.getNumberOfColumns} from $partFile")
        }
        Some(table)
      }
    } finally {
      dataBuffer.close()
    }
  }
}
