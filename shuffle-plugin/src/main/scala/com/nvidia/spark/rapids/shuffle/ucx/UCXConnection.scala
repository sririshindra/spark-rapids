/*
 * Copyright (c) 2020-2021, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.shuffle.ucx

import java.io.{InputStream, OutputStream}
import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentHashMap

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.MemoryBuffer
import com.nvidia.spark.rapids.Arm
import com.nvidia.spark.rapids.shuffle._
import org.openucx.jucx.UcxCallback
import org.openucx.jucx.ucp.UcpRequest

import org.apache.spark.internal.Logging

/**
 * These are private apis used within the ucx package.
 */

/**
 * `UCXAmCallback` is used by [[Transaction]] to handle UCX Active Messages operations.
 * The `UCXActiveMessage` object encapsulates an activeMessageId and a header.
 */
private[ucx] abstract class UCXAmCallback {
  def onError(am: UCXActiveMessage, ucsStatus: Int, errorMsg: String): Unit
  def onMessageStarted(receiveAm: UcpRequest): Unit
  def onSuccess(am: UCXActiveMessage, buff: TransportBuffer): Unit
  def onCancel(am: UCXActiveMessage): Unit
  def onMessageReceived(size: Long, header: Long, finalizeCb: TransportBuffer => Unit): Unit
}

class UCXServerConnection(ucx: UCX, transport: UCXShuffleTransport)
  extends UCXConnection(ucx) with ServerConnection with Logging with Arm {
  override def startManagementPort(host: String): Int = {
    ucx.startManagementPort(host)
  }

  override def registerRequestHandler(messageType: MessageType.Value,
      cb: TransactionCallback): Unit = {
    ucx.registerRequestHandler(
      UCXConnection.composeRequestAmId(messageType), () =>
        new UCXAmCallback {
          private val tx = createTransaction
          tx.start(UCXTransactionType.Request, 1, cb)

          override def onSuccess(am: UCXActiveMessage, buff: TransportBuffer): Unit = {
            logDebug(s"At requestHandler for ${messageType} and am: " +
              s"$am")
            tx.completeWithSuccess(messageType, Option(am.header), Option(buff))
          }

          override def onMessageReceived(size: Long, header: Long,
                                         finalizeCb: (TransportBuffer => Unit)): Unit = {
            logDebug(s"onMessageReceived for hdr ${TransportUtils.toHex(header)}")
            finalizeCb(new MetadataTransportBuffer(transport.getDirectByteBuffer(size)))
          }

          override def onError(am: UCXActiveMessage, ucsStatus: Int, errorMsg: String): Unit = {
            tx.completeWithError(errorMsg)
          }

          override def onCancel(am: UCXActiveMessage): Unit = {
            tx.completeCancelled(messageType, am.header)
          }

          override def onMessageStarted(receiveAm: UcpRequest): Unit = {
            tx.registerPendingMessage(receiveAm)
          }
        })
  }

  override def send(peerExecutorId: Long,
      messageType: MessageType.Value,
      header: Long,
      buffer: MemoryBuffer,
      cb: TransactionCallback): Transaction = {
    val tx = createTransaction
    tx.start(UCXTransactionType.Send, 1, cb)
    logDebug(s"Sending to ${peerExecutorId} at ${TransportUtils.toHex(header)} " +
      s"with ${buffer}")

    val sendAm = UCXActiveMessage(UCXConnection.composeSendAmId(messageType),
      header, forceRndv = true)

    ucx.sendActiveMessage(peerExecutorId, sendAm, buffer,
      new UcxCallback {
        override def onSuccess(request: UcpRequest): Unit = {
          logDebug(s"AM success send $sendAm")
          tx.complete(TransactionStatus.Success)
        }

        override def onError(ucsStatus: Int, errorMsg: String): Unit = {
          logDebug(s"AM Error sending ${ucsStatus} ${errorMsg} for $sendAm")
          tx.completeWithError(errorMsg)
        }
      })
    tx
  }

  override def send(peerExecutorId: Long,
      messageType: MessageType.Value,
      header: Long,
      response: ByteBuffer,
      cb: TransactionCallback): Transaction = {
    val tx = createTransaction
    tx.start(UCXTransactionType.Request, 1, cb)

    logDebug(s"Responding to ${peerExecutorId} at ${TransportUtils.toHex(header)} " +
      s"with ${response}")

    val responseAm = UCXActiveMessage(
      UCXConnection.composeResponseAmId(messageType), header, false)
    ucx.sendActiveMessage(peerExecutorId, responseAm, response,
      new UcxCallback {
        override def onSuccess(request: UcpRequest): Unit = {
          logDebug(s"AM success respond $responseAm")
          tx.complete(TransactionStatus.Success)
        }

        override def onError(ucsStatus: Int, errorMsg: String): Unit = {
          logError(s"AM Error responding to peer ${peerExecutorId} " +
            s"status=${ucsStatus} msg=${errorMsg} for $responseAm")
          tx.completeWithError(errorMsg)
        }
      })
    tx
  }
}

class UCXClientConnection(peerExecutorId: Int, ucx: UCX, transport: UCXShuffleTransport)
  extends UCXConnection(peerExecutorId, ucx) with Arm
  with ClientConnection {

  override def toString: String = {
    s"UCXClientConnection(ucx=$ucx, " +
      s"peerExecutorId=$peerExecutorId)"
  }

  logInfo(s"UCX Client $this started")

  override def getPeerExecutorId: Long = peerExecutorId

  override def request(messageType: MessageType.Value, request: ByteBuffer,
                       cb: TransactionCallback): Transaction = {
    val tx = createTransaction
    tx.start(UCXTransactionType.Request, 1, cb)

    // this header is unique, so we can send it with the request
    // expecting it to be echoed back in the response
    val requestHeader = UCXConnection.composeRequestHeader(ucx.getExecutorId.toLong, tx.txId)

    // This is the response active message handler, when the response shows up
    // we'll create a transaction, and set header/message and complete it.
    val amCallback = new UCXAmCallback {
      override def onMessageReceived(size: Long, header: Long,
                                     finalizeCb: (TransportBuffer => Unit)): Unit = {
        finalizeCb(new MetadataTransportBuffer(transport.getDirectByteBuffer(size.toInt)))
      }

      override def onSuccess(am: UCXActiveMessage, buff: TransportBuffer): Unit = {
        tx.completeWithSuccess(messageType, Option(am.header), Option(buff))
      }

      override def onError(am: UCXActiveMessage, ucsStatus: Int, errorMsg: String): Unit = {
        tx.completeWithError(errorMsg)
      }

      override def onMessageStarted(receiveAm: UcpRequest): Unit = {
        tx.registerPendingMessage(receiveAm)
      }

      override def onCancel(am: UCXActiveMessage): Unit = {
        tx.completeCancelled(messageType, am.header)
      }
    }

    // Register the active message response handler. Note that the `requestHeader`
    // is expected to come back with the response, and is used to find the
    // correct callback (this is an implementation detail in UCX.scala)
    ucx.registerResponseHandler(
      UCXConnection.composeResponseAmId(messageType), requestHeader, amCallback)

    // kick-off the request
    val requestAm = UCXActiveMessage(
      UCXConnection.composeRequestAmId(messageType), requestHeader, false)

    logDebug(s"Performing a ${messageType} request of size ${request.remaining()} " +
      s"with tx ${tx}. Active messages: request $requestAm")

    ucx.sendActiveMessage(peerExecutorId, requestAm, request,
      new UcxCallback {
        override def onError(ucsStatus: Int, errorMsg: String): Unit = {
          tx.completeWithError(errorMsg)
        }

        // we don't handle `onSuccess` here, because we want the response
        // to complete that
      })

    tx
  }

  override def registerReceiveHandler(receiveType: MessageType.Value): Unit = {
    require(receiveType == MessageType.Buffer,
      "The only receive types that are currently allowed are of type `Buffer`")
    val hdrWildcard = UCXConnection.composeBufferHeader(peerExecutorId, 0L)
    // register a handler that must match "sends" from a peer, hence the use of `composeSendAmId`
    ucx.registerReceiveHandler(UCXConnection.composeSendAmId(receiveType),
      UCXConnection.upperBitsMask, hdrWildcard, () => new UCXAmCallback {
        private val tx = createTransaction
        tx.start(UCXTransactionType.Receive, 1, transport.handleBufferTransaction)

        override def onError(am: UCXActiveMessage, ucsStatus: Int, errorMsg: String): Unit = {
          tx.completeWithError(errorMsg)
        }

        override def onMessageStarted(receiveAm: UcpRequest): Unit = {
          tx.registerPendingMessage(receiveAm)
        }

        override def onSuccess(am: UCXActiveMessage, buff: TransportBuffer): Unit = {
          // the buffer doesn't belong to this transaction, hence the last argument is None.
          tx.completeWithSuccess(MessageType.Buffer, Option(am.header), None)
        }

        override def onCancel(am: UCXActiveMessage): Unit = {
          tx.completeCancelled(MessageType.Buffer, am.header)
        }

        override def onMessageReceived(size: Long, header: Long,
                                       finalizeCb: TransportBuffer => Unit): Unit = {
          logDebug(s"Received message from ${peerExecutorId} size ${size} " +
            s"header ${TransportUtils.toHex(header)}")
          transport.handleBufferReceive(size, header, finalizeCb)
        }
      })
  }
}

class UCXConnection(peerExecutorId: Int, val ucx: UCX) extends Logging {
  // alternate constructor for a server connection (-1 because it is not to a peer)
  def this(ucx: UCX) = this(-1, ucx)

  private[this] val pendingTransactions = new ConcurrentHashMap[Long, UCXTransaction]()

  private[ucx] def cancel(msg: UcpRequest): Unit =
    ucx.cancel(msg)

  private[ucx] def createTransaction: UCXTransaction = {
    val thisTxId = ucx.getNextTransactionId

    val tx = new UCXTransaction(this, thisTxId)

    pendingTransactions.put(thisTxId, tx)
    logDebug(s"PENDING TRANSACTIONS AFTER ADD $pendingTransactions")
    tx
  }

  def removeTransaction(tx: UCXTransaction): Unit = {
    pendingTransactions.remove(tx.txId)
    logDebug(s"PENDING TRANSACTIONS AFTER REMOVE $pendingTransactions")
  }

  override def toString: String = {
    s"Connection(ucx=$ucx, peerExecutorId=$peerExecutorId) "
  }
}

object UCXConnection extends Logging {
  /**
   * 1) client gets upper 28 bits
   * 2) then comes the type, which gets 4 bits
   * 3) the remaining 32 bits are used for buffer specific headers
   */
  private final val bufferMsgType: Long = 0x0000000000000000BL

  // Mask for message types. The only message type used is `bufferMsgType`
  private final val msgTypeMask: Long   = 0x000000000000000FL

  // UCX Active Message masks (we can use up to 5 bits for these ids)
  private final val amRequestMask: Int  = 0x0000000F
  private final val amSendMask: Int = 0x0000001F

  // We pick the 5th bit set to 1 as a "send" active message
  private final val amSendFlag: Int = 0x00000010

  // pick up the lower and upper parts of a long
  private final val lowerBitsMask: Long = 0x00000000FFFFFFFFL
  final val upperBitsMask: Long = 0xFFFFFFFF00000000L

  def composeBufferHeader(peerClientId: Long, uniqueId: Long): Long = {
    // buffer headers are [peerClientId, 0, uniqueId]
    val shiftedUniqueId = uniqueId << 16
    composeHeader(composeUpperBits(peerClientId, bufferMsgType), shiftedUniqueId)
  }

  def composeRequestAmId(messageType: MessageType.Value): Int =
    composeBaseAmId(messageType)

  // we use the `amSendFlag` to signify messages that either sent from the server
  // for buffer sends and meta replies, or the receive on the client side for
  // either buffer or meta reply receives.
  def composeResponseAmId(messageType: MessageType.Value): Int =
    composeSendAmId(messageType)

  private def composeBaseAmId(messageType: MessageType.Value): Int = {
    val amId = messageType.id
    if ((amId & amRequestMask) != amId) {
      throw new IllegalArgumentException(
        s"Invalid request amId, it must be 4 bits: ${TransportUtils.toHex(amId)}")
    }
    amId
  }

  def composeSendAmId(messageType: MessageType.Value): Int = {
    val amId = amSendFlag | composeBaseAmId(messageType)
    if ((amId & amSendMask) != amId) {
      throw new IllegalArgumentException(
        s"Invalid amId, it must be 5 bits: ${TransportUtils.toHex(amId)}")
    }
    amId
  }

  def composeHeader(upperBits: Long, lowerBits: Long): Long = {
    if ((upperBits & upperBitsMask) != upperBits) {
      throw new IllegalArgumentException(
        s"Invalid header, upperBits would alias: ${TransportUtils.toHex(upperBits)}")
    }
    // the lower 32bits aliasing is not a big deal, we expect it with msg rollover
    // so we don't check for it
    upperBits | (lowerBits & lowerBitsMask)
  }

  private def composeUpperBits(peerClientId: Long, msgType: Long): Long = {
    if ((peerClientId & lowerBitsMask) != peerClientId) {
      throw new IllegalArgumentException(
        s"Invalid header, peerClientId would alias: ${TransportUtils.toHex(peerClientId)}")
    }
    if ((msgType & msgTypeMask) != msgType) {
      throw new IllegalArgumentException(
        s"Invalid header, msgType would alias: ${TransportUtils.toHex(msgType)}")
    }
    (peerClientId << 36) | (msgType << 32)
  }

  def composeRequestHeader(executorId: Long, txId: Long): Long = {
    require(executorId >= 0,
      s"Attempted to pack negative $executorId")
    require((executorId & lowerBitsMask) == executorId,
        s"ExecutorId would alias: ${TransportUtils.toHex(executorId)}")
    composeHeader(executorId << 32, txId)
  }

  def extractExecutorId(header: Long): Long = {
    (header >> 32) & lowerBitsMask
  }
  //
  // Handshake message code. This, I expect, could be folded into the [[BlockManagerId]],
  // but I have not tried this. If we did, it would eliminate the extra TCP connection
  // in this class.
  //
  private def readBytesFromStream(direct: Boolean,
    is: InputStream, lengthToRead: Int): ByteBuffer = {
    var bytesRead = 0
    var read = 0
    val buff = new Array[Byte](lengthToRead)
    while (read >= 0 && bytesRead < lengthToRead) {
      logTrace(s"Reading ${lengthToRead}. Currently at ${bytesRead}")
      read = is.read(buff, bytesRead, lengthToRead - bytesRead)
      if (read > 0) {
        bytesRead = bytesRead + read
      }
    }

    if (bytesRead < lengthToRead) {
      throw new IllegalStateException("Read less bytes than expected!")
    }

    val byteBuffer = ByteBuffer.wrap(buff)

    if (direct) {
      // a direct buffer does not allow .array() (not implemented).
      // therefore we received in the JVM heap, and now we copy to the native heap
      // using a .put on that native buffer
      val directCopy = ByteBuffer.allocateDirect(lengthToRead)
      TransportUtils.copyBuffer(byteBuffer, directCopy, lengthToRead)
      directCopy.rewind() // reset position
      directCopy
    } else {
      byteBuffer
    }
  }

  /**
   * Given a java `InputStream`, obtain the peer's `WorkerAddress` and executor id,
   * returning them as a pair.
   *
   * @param is management port input stream
   * @return a tuple of worker address, the peer executor id, and rkeys
   */
  def readHandshakeHeader(is: InputStream): (WorkerAddress, Int, Rkeys)  = {
    val maxLen = 1024 * 1024

    // get the length from the stream, it's the first thing sent.
    val workerAddressLength = readBytesFromStream(false, is, 4).getInt()

    require(workerAddressLength <= maxLen,
      s"Received an abnormally large (>$maxLen Bytes) WorkerAddress " +
        s"(${workerAddressLength} Bytes), dropping.")

    val workerAddress = readBytesFromStream(true, is, workerAddressLength)

    // get the remote executor Id, that's the last part of the handshake
    val executorId = readBytesFromStream(false, is, 4).getInt()

    // get the number of rkeys expected next
    val numRkeys = readBytesFromStream(false, is, 4).getInt()

    val rkeys = new ArrayBuffer[ByteBuffer](numRkeys)
    (0 until numRkeys).foreach { _ =>
      val size = readBytesFromStream(false, is, 4).getInt()
      rkeys.append(readBytesFromStream(true, is, size))
    }

    (WorkerAddress(workerAddress), executorId, Rkeys(rkeys))
  }

  /**
   * Writes a header that is exchanged in the management port. The header contains:
   *  - UCP Worker address length (4 bytes)
   *  - UCP Worker address (variable length)
   *  - Local executor id (4 bytes)
   *  - Local rkeys count (4 bytes)
   *  - Per rkey: rkey length (4 bytes) + rkey payload (variable)
   *
   * @param os OutputStream to write to
   * @param workerAddress byte buffer that holds the local UCX worker address
   * @param localExecutorId The local executorId
   * @param rkeys The local rkeys to send to the peer
   */
  def writeHandshakeHeader(os: OutputStream,
                           workerAddress: ByteBuffer,
                           localExecutorId: Int,
                           rkeys: Seq[ByteBuffer]): Unit = {
    val headerSize = 4 + workerAddress.remaining() + 4 +
        4 + (4 * rkeys.size) + (rkeys.map(_.capacity).sum)
    val hsBuff = ByteBuffer.allocate(headerSize)

    // pack the worker address
    hsBuff.putInt(workerAddress.capacity)
    hsBuff.put(workerAddress)

    // send the executor id
    hsBuff.putInt(localExecutorId)

    // pack the rkeys
    hsBuff.putInt(rkeys.size)
    rkeys.foreach { rkey =>
      hsBuff.putInt(rkey.capacity)
      hsBuff.put(rkey)
    }
    hsBuff.flip()
    os.write(hsBuff.array)
    os.flush()
  }
}
