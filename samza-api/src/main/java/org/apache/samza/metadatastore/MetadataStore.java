/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.samza.metadatastore;

import org.apache.samza.annotation.InterfaceStability;
import org.apache.samza.container.SamzaContainerContext;

import java.util.Map;

/**
 * Store abstraction responsible for managing the metadata of a Samza job and is agnostic of the
 * deployment model (yarn/standalone) of the Samza job.
 */
@InterfaceStability.Evolving
public interface MetadataStore {

  /**
   * Initializes the metadata store, if applicable, setting up the underlying resources
   * and connections to the store endpoints. Upon successful completion of this method,
   * metadata store is considered available to accept the client operations.
   *
   * @param containerContext represents the Samza container context.
   */
  void init(SamzaContainerContext containerContext);

  /**
   * TODO: Fix the doc.
   * @param key denotes the key for which value has to be retrieved.
   * @return the value.
   */
  byte[] get(byte[] key);

  /**
   * TODO: Fix the doc.
   * @param key key to update.
   * @param value value to update.
   */
  void put(byte[] key, byte[] value);

  /**
   * Removes the key from the KV store.
   * @param key denotes the key to be deleted.
   */
  void remove(byte[] key);

  /**
   *
   * @return all the (key, value) pairs in the store.
   */
  Map<byte[], byte[]> all();

  /**
   * Closes the metadata store, if applicable, relinquishing all the underlying resources
   * and connections.
   */
  void close();
}
