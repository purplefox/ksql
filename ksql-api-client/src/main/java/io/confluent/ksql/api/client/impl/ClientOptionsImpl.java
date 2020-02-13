/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.api.client.impl;

import io.confluent.ksql.api.client.ClientOptions;

public class ClientOptionsImpl implements ClientOptions {

  private String host = "localhost";
  private int port = 8089;
  private boolean useTls = true;
  private boolean useClientAuth = false;

  public ClientOptionsImpl() {
  }

  private ClientOptionsImpl(final String host, final int port, final boolean useTls,
      final boolean useClientAuth) {
    this.host = host;
    this.port = port;
    this.useTls = useTls;
    this.useClientAuth = useClientAuth;
  }

  public ClientOptions copy() {
    return new ClientOptionsImpl(host, port, useTls, useClientAuth);
  }

  @Override
  public ClientOptions setHost(final String host) {
    this.host = host;
    return this;
  }

  @Override
  public ClientOptions setPort(final int port) {
    this.port = port;
    return this;
  }

  @Override
  public ClientOptions setUseTls(final boolean useTls) {
    this.useTls = useTls;
    return this;
  }

  @Override
  public ClientOptions setUseClientAuth(final boolean useClientAuth) {
    this.useClientAuth = useClientAuth;
    return this;
  }

  @Override
  public String getHost() {
    return host;
  }

  @Override
  public int getPort() {
    return port;
  }

  @Override
  public boolean isUseTls() {
    return useTls;
  }

  @Override
  public boolean isUseClientAuth() {
    return useClientAuth;
  }
}
