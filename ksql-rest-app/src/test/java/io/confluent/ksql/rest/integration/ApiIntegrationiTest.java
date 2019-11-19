/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.rest.integration;

import static io.confluent.ksql.test.util.EmbeddedSingleNodeKafkaCluster.VALID_USER1;
import static io.confluent.ksql.test.util.EmbeddedSingleNodeKafkaCluster.VALID_USER2;
import static io.confluent.ksql.test.util.EmbeddedSingleNodeKafkaCluster.ops;
import static io.confluent.ksql.test.util.EmbeddedSingleNodeKafkaCluster.prefixedResource;
import static io.confluent.ksql.test.util.EmbeddedSingleNodeKafkaCluster.resource;
import static org.apache.kafka.common.acl.AclOperation.ALL;
import static org.apache.kafka.common.acl.AclOperation.CREATE;
import static org.apache.kafka.common.acl.AclOperation.DESCRIBE;
import static org.apache.kafka.common.acl.AclOperation.DESCRIBE_CONFIGS;
import static org.apache.kafka.common.acl.AclOperation.WRITE;
import static org.apache.kafka.common.resource.ResourceType.CLUSTER;
import static org.apache.kafka.common.resource.ResourceType.GROUP;
import static org.apache.kafka.common.resource.ResourceType.TOPIC;
import static org.apache.kafka.common.resource.ResourceType.TRANSACTIONAL_ID;
import static org.junit.Assert.assertEquals;

import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.api.client.KSqlClient;
import io.confluent.ksql.api.client.Row;
import io.confluent.ksql.api.flow.Subscriber;
import io.confluent.ksql.api.flow.Subscription;
import io.confluent.ksql.integration.IntegrationTestHarness;
import io.confluent.ksql.rest.server.TestKsqlRestApp;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.test.util.EmbeddedSingleNodeKafkaCluster;
import io.confluent.ksql.test.util.secure.ClientTrustStore;
import io.confluent.ksql.test.util.secure.Credentials;
import io.confluent.ksql.test.util.secure.SecureKafkaHelper;
import io.confluent.ksql.util.PageViewDataProvider;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;

@Category({IntegrationTest.class})
public class ApiIntegrationiTest {

  private static final int HEADER = 1;  // <-- some responses include a header as the first message.
  private static final int FOOTER = 1;  // <-- some responses include a footer as the last message.
  private static final int LIMIT = 2;
  private static final String PAGE_VIEW_TOPIC = "pageviews";
  private static final String PAGE_VIEW_STREAM = "pageviews_original";
  private static final String AGG_TABLE = "AGG_TABLE";
  private static final Credentials SUPER_USER = VALID_USER1;
  private static final Credentials NORMAL_USER = VALID_USER2;
  private static final String AN_AGG_KEY = "USER_1";

  private static final IntegrationTestHarness TEST_HARNESS = IntegrationTestHarness.builder()
      .withKafkaCluster(
          EmbeddedSingleNodeKafkaCluster.newBuilder()
              .withoutPlainListeners()
              .withSaslSslListeners()
              .withAclsEnabled(SUPER_USER.username)
              .withAcl(
                  NORMAL_USER,
                  resource(CLUSTER, "kafka-cluster"),
                  ops(DESCRIBE_CONFIGS, CREATE)
              )
              .withAcl(
                  NORMAL_USER,
                  prefixedResource(TOPIC, "_confluent-ksql-default_"),
                  ops(ALL)
              )
              .withAcl(
                  NORMAL_USER,
                  resource(TOPIC, PAGE_VIEW_TOPIC),
                  ops(ALL)
              )
              .withAcl(
                  NORMAL_USER,
                  prefixedResource(GROUP, "_confluent-ksql-default_transient_"),
                  ops(ALL)
              )
              .withAcl(
                  NORMAL_USER,
                  prefixedResource(GROUP, "_confluent-ksql-default_query"),
                  ops(ALL)
              )
              .withAcl(
                  NORMAL_USER,
                  resource(TOPIC, "X"),
                  ops(ALL)
              )
              .withAcl(
                  NORMAL_USER,
                  resource(TOPIC, "AGG_TABLE"),
                  ops(ALL)
              )
              .withAcl(
                  NORMAL_USER,
                  resource(TRANSACTIONAL_ID, "default_"),
                  ops(WRITE)
              )
              .withAcl(
                  NORMAL_USER,
                  resource(TRANSACTIONAL_ID, "default_"),
                  ops(DESCRIBE)
              ).withAcl(
              NORMAL_USER,
              resource(TOPIC, "__consumer_offsets"),
              ops(DESCRIBE)
          ).withAcl(
              NORMAL_USER,
              resource(TOPIC, "__transaction_state"),
              ops(DESCRIBE)
          )
      )
      .build();

  private static final TestKsqlRestApp REST_APP = TestKsqlRestApp
      .builder(TEST_HARNESS::kafkaBootstrapServers)
      .withProperty("security.protocol", "SASL_SSL")
      .withProperty("sasl.mechanism", "PLAIN")
      .withProperty("sasl.jaas.config", SecureKafkaHelper.buildJaasConfig(NORMAL_USER))
      .withProperties(ClientTrustStore.trustStoreProps())
      .build();

  @ClassRule
  public static final RuleChain CHAIN = RuleChain.outerRule(TEST_HARNESS).around(REST_APP);

  private KSqlClient client;

  @BeforeClass
  public static void setUpClass() {
    TEST_HARNESS.ensureTopics(PAGE_VIEW_TOPIC);

    TEST_HARNESS.produceRows(PAGE_VIEW_TOPIC, new PageViewDataProvider(), Format.JSON);

    RestIntegrationTestUtil.createStreams(REST_APP, PAGE_VIEW_STREAM, PAGE_VIEW_TOPIC);

    makeKsqlRequest("CREATE TABLE " + AGG_TABLE + " AS "
        + "SELECT COUNT(1) AS COUNT FROM " + PAGE_VIEW_STREAM + " GROUP BY USERID;");
  }

  @Before
  public void setUp() {
    client = KSqlClient.client();
  }

  @After
  public void tearDown() {
  }

  @Test
  public void testExecuteQuery() throws Throwable {

    CompletableFuture<List<Row>> queryFut =
        client.connectWebsocket("localhost", 8888)
            .thenCompose(con -> con.executeQuery(
                "SELECT * from " + PAGE_VIEW_STREAM + " EMIT CHANGES LIMIT " + LIMIT + ";"));

    List<Row> items = queryFut.get();
    System.out.println("**** Got rows:");
    for (Row row : items) {
      System.out.println(row);
    }
    //assertEquals(10, items.size());
    System.out.println(items);
  }

  @Test
  public void testStreamPushQuery() throws Throwable {

    TestSubscriber<Row> subscriber = new TestSubscriber<>();

    CompletableFuture<Integer> queryFut =
        client.connectWebsocket("localhost", 8888)
            .thenCompose(con -> con.streamQuery(
                "SELECT * from " + PAGE_VIEW_STREAM + " EMIT CHANGES LIMIT " + LIMIT + ";", false,
                subscriber));

    Integer res = queryFut.get();
    assertEquals(123, res.intValue());

    List<Row> items = subscriber.waitForItems(2, 10000);
    assertEquals(2, items.size());
    System.out.println(items);
  }

  private static void makeKsqlRequest(final String sql) {
    RestIntegrationTestUtil.makeKsqlRequest(REST_APP, sql);
  }


  static class TestSubscriber<T> implements Subscriber<T> {

    private final List<T> items = new ArrayList<>();
    private Throwable error;
    private boolean completed;
    private Subscription subscription;

    @Override
    public synchronized void onNext(T item) {
      if (subscription == null) {
        throw new IllegalStateException("subscription has not been set");
      }
      subscription.request(1);
      items.add(item);
    }

    @Override
    public synchronized void onError(Throwable e) {
      error = e;
    }

    @Override
    public synchronized void onComplete() {
      completed = true;
    }

    @Override
    public synchronized void onSubscribe(Subscription subscription) {
      this.subscription = subscription;
      subscription.request(1);
    }

    public List<T> waitForItems(int num, long timeout) throws Exception {
      long start = System.currentTimeMillis();
      while ((System.currentTimeMillis() - start) < timeout) {
        synchronized (this) {
          if (items.size() >= num) {
            return items;
          }
        }
        Thread.sleep(10);
      }
      throw new TimeoutException("Timed out waiting for items");
    }

  }

}
