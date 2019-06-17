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
package org.apache.samza.zk;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPOutputStream;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.serializers.model.SamzaObjectMapper;
import org.codehaus.jackson.map.ObjectMapper;
import org.xerial.snappy.Snappy;

public class JobModelCompressionUtil {

  public static long getGzipCompressedJobModelSize(JobModel jobModel) throws IOException {
    byte[] jobModelByteArray = convertJobModelToBytes(jobModel);
    ByteArrayOutputStream baos = new ByteArrayOutputStream(jobModelByteArray.length);
    GZIPOutputStream gzipOS = new GZIPOutputStream(baos);
    gzipOS.write(jobModelByteArray, 0, jobModelByteArray.length);
    gzipOS.finish();
    byte[] gzippedByteArray = baos.toByteArray();
    return gzippedByteArray.length;
  }

  public static byte[] convertJobModelToBytes(JobModel jobModel) throws IOException {
    ObjectMapper mapper = SamzaObjectMapper.getObjectMapper();
    String serializedJobModelAsString = mapper.writeValueAsString(jobModel);
    return serializedJobModelAsString.getBytes("UTF-8");
  }

  public static long getSnappyCompressedObjectSize(JobModel jobModel) throws Exception {
    byte[] jobModelAsBytes = convertJobModelToBytes(jobModel);
    byte[] snappyCompressedJobModelAsBytes = Snappy.compress(jobModelAsBytes);
    return snappyCompressedJobModelAsBytes.length;
  }
}
