/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.internal.statistics;

import java.io.File;
import java.util.Optional;

import org.apache.geode.logging.internal.spi.LogFile;

/**
 * Defines the contract enabling the {@link StatArchiveHandler} to retrieve configuration details
 * (some of which may change at runtime).
 *
 * <p>
 * Implemented by {@link HostStatSampler}.
 *
 * <p>
 * Note: This interface changes the return types of file limit properties from int to long so that
 * the Stat Sampler tests can change the units from MB to KB.
 *
 * @since GemFire 7.0
 */
public interface StatArchiveHandlerConfig {

  /**
   * Gets the name of the archive file.
   */
  File getArchiveFileName();

  /**
   * Gets the archive size limit in bytes.
   */
  long getArchiveFileSizeLimit();

  /**
   * Gets the archive disk space limit in bytes.
   */
  long getArchiveDiskSpaceLimit();

  /**
   * Returns a unique id for the sampler's system.
   */
  long getSystemId();

  /**
   * Returns the time this sampler's system was started.
   */
  long getSystemStartTime();

  /**
   * Returns the path to this sampler's system directory; if it has one.
   */
  String getSystemDirectoryPath();

  /**
   * Returns a description of the product that the stats are on
   */
  String getProductDescription();

  /**
   * Returns the log file to provide info about main id and child id.
   */
  Optional<LogFile> getLogFile();
}
