package redust.clienttests;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.protocol.ProtocolVersion;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class LettuceCompatTest {

  private static RedisURI uri() {
    String addr = System.getenv().getOrDefault("REDUST_ADDR", "127.0.0.1:6379");
    String[] parts = addr.split(":", 2);
    String host = parts[0];
    int port = Integer.parseInt(parts[1]);

    Optional<String> password = Optional.ofNullable(System.getenv("REDUST_PASSWORD"));

    RedisURI.Builder b = RedisURI.builder()
        .withHost(host)
        .withPort(port)
        .withTimeout(Duration.ofSeconds(2));

    password.ifPresent(p -> b.withPassword(p.toCharArray()));
    return b.build();
  }

  @Test
  public void asyncBasicCommandsRoundtrip() throws Exception {
    RedisClient client = RedisClient.create(uri());
    client.setOptions(ClientOptions.builder().protocolVersion(ProtocolVersion.RESP2).build());
    StatefulRedisConnection<String, String> conn = client.connect();
    try {
      RedisAsyncCommands<String, String> async = conn.async();

      String pong = async.ping().get(2, TimeUnit.SECONDS);
      assertEquals("PONG", pong);

      String key = "lettuce:basic:key";
      async.del(key).get(2, TimeUnit.SECONDS);

      String ok = async.set(key, "bar").get(2, TimeUnit.SECONDS);
      assertEquals("OK", ok);

      String v = async.get(key).get(2, TimeUnit.SECONDS);
      assertEquals("bar", v);

      String cntKey = "lettuce:basic:cnt";
      async.del(cntKey).get(2, TimeUnit.SECONDS);
      async.set(cntKey, "0").get(2, TimeUnit.SECONDS);

      Long v1 = async.incr(cntKey).get(2, TimeUnit.SECONDS);
      assertEquals(1L, v1);

      Long v2 = async.incrby(cntKey, 5).get(2, TimeUnit.SECONDS);
      assertEquals(6L, v2);
    } finally {
      conn.close();
      client.shutdown();
    }
  }

  @Test
  public void connectionReuseIsStable() throws Exception {
    RedisClient client = RedisClient.create(uri());
    client.setOptions(ClientOptions.builder().protocolVersion(ProtocolVersion.RESP2).build());
    StatefulRedisConnection<String, String> conn = client.connect();
    try {
      RedisAsyncCommands<String, String> async = conn.async();

      String key = "lettuce:reuse:key";
      async.del(key).get(2, TimeUnit.SECONDS);

      for (int i = 0; i < 20; i++) {
        String expected = "v" + i;
        String ok = async.set(key, expected).get(2, TimeUnit.SECONDS);
        assertEquals("OK", ok);

        String got = async.get(key).get(2, TimeUnit.SECONDS);
        assertEquals(expected, got);
      }

      // 并发 future：验证同一连接异步请求不会乱序/卡死（最小覆盖）
      CompletableFuture<String> f1 = async.get(key).toCompletableFuture();
      CompletableFuture<String> f2 = async.get(key).toCompletableFuture();
      assertNotNull(f1.get(2, TimeUnit.SECONDS));
      assertNotNull(f2.get(2, TimeUnit.SECONDS));
    } finally {
      conn.close();
      client.shutdown();
    }
  }
}
