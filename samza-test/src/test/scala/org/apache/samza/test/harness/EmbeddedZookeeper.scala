/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.samza.test.harness

import org.apache.zookeeper.server.ZooKeeperServer
import org.apache.zookeeper.server.NIOServerCnxnFactory
import kafka.utils.TestUtils
import java.net.InetSocketAddress

import org.slf4j.{LoggerFactory, MDC}
import kafka.utils.CoreUtils
import kafka.zk.ZkFourLetterWords
import org.apache.kafka.common.utils.Utils

class EmbeddedZookeeper() {
  val loggerName = this.getClass.getName
  lazy val logger = LoggerFactory.getLogger(loggerName)
  val snapshotDir = TestUtils.tempDir()
  val logDir = TestUtils.tempDir()
  val tickTime = 500
  val zookeeper = new ZooKeeperServer(snapshotDir, logDir, tickTime)
  val factory = new NIOServerCnxnFactory()
  private val addr = new InetSocketAddress("127.0.0.1", TestUtils.RandomPort)
  factory.configure(addr, 0)
  factory.startup(zookeeper)
  val port = zookeeper.getClientPort()

  def shutdown() {
    CoreUtils.swallow(zookeeper.shutdown())
    CoreUtils.swallow(factory.shutdown())

    def isDown(): Boolean = {
      try {
        logger.info("Sending stat request from EmbeddedZookeeper during shutdown.")
        ZkFourLetterWords.sendStat("127.0.0.1", port, 3000)
        logger.info("Zookeeper server is alive.")
        false
      } catch { case _: Throwable => true }
    }

    Iterator.continually(isDown()).exists(identity)

    Utils.delete(logDir)
    Utils.delete(snapshotDir)
  }

}
