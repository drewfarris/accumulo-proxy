/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.proxy.rpc;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.ClosedByInterruptException;
import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.rpc.TraceProtocolFactory;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TTransportFactory;
import org.apache.thrift.transport.layered.TFramedTransport;

public class ThriftUtil {

  private static final TraceProtocolFactory protocolFactory = new TraceProtocolFactory();
  private static final TFramedTransport.Factory transportFactory =
      new TFramedTransport.Factory(Integer.MAX_VALUE);
  private static final Map<Integer,TTransportFactory> factoryCache = new HashMap<>();

  public static final String GSSAPI = "GSSAPI", DIGEST_MD5 = "DIGEST-MD5";

  /**
   * An instance of {@link TraceProtocolFactory}
   *
   * @return The default Thrift TProtocolFactory for RPC
   */
  public static TProtocolFactory protocolFactory() {
    return protocolFactory;
  }

  /**
   * An instance of {@link org.apache.thrift.transport.layered.TFramedTransport.Factory}
   *
   * @return The default Thrift TTransportFactory for RPC
   */
  public static TTransportFactory transportFactory() {
    return transportFactory;
  }

  /**
   * Get an instance of the TTransportFactory with the provided maximum frame size
   *
   * @param maxFrameSize Maximum Thrift message frame size
   * @return A, possibly cached, TTransportFactory with the requested maximum frame size
   */
  public static synchronized TTransportFactory transportFactory(long maxFrameSize) {
    if (maxFrameSize > Integer.MAX_VALUE || maxFrameSize < 1) {
      throw new RuntimeException("Thrift transport frames are limited to " + Integer.MAX_VALUE);
    }
    int maxFrameSize1 = (int) maxFrameSize;
    TTransportFactory factory = factoryCache.get(maxFrameSize1);
    if (factory == null) {
      factory = new TFramedTransport.Factory(maxFrameSize1);
      factoryCache.put(maxFrameSize1, factory);
    }
    return factory;
  }

  public static void checkIOExceptionCause(IOException e) {
    if (e instanceof ClosedByInterruptException) {
      Thread.currentThread().interrupt();
      throw new UncheckedIOException(e);
    }
  }
}
