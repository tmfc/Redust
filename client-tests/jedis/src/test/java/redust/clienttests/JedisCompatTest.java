package redust.clienttests;

import org.junit.jupiter.api.Test;
import redis.clients.jedis.ConnectionPoolConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

import java.util.Locale;

import static org.junit.jupiter.api.Assertions.*;

public class JedisCompatTest {

  private static HostAndPort getAddr() {
    String addr = System.getenv().getOrDefault("REDUST_ADDR", "127.0.0.1:6379");
    String[] parts = addr.split(":", 2);
    String host = parts[0];
    int port = Integer.parseInt(parts[1]);
    return new HostAndPort(host, port);
  }

  @Test
  public void basicCommandsRoundtrip() {
    HostAndPort hp = getAddr();

    try (Jedis jedis = new Jedis(hp.getHost(), hp.getPort())) {
      String pong = jedis.ping();
      assertEquals("PONG", pong);

      String key = "jedis:basic:key";
      jedis.del(key);

      String ok = jedis.set(key, "bar");
      assertEquals("OK", ok);

      String v = jedis.get(key);
      assertEquals("bar", v);

      String cntKey = "jedis:basic:cnt";
      jedis.del(cntKey);
      jedis.set(cntKey, "0");

      long v1 = jedis.incr(cntKey);
      assertEquals(1L, v1);

      long v2 = jedis.incrBy(cntKey, 5);
      assertEquals(6L, v2);
    }
  }

  @Test
  public void pipelineRoundtrip() {
    HostAndPort hp = getAddr();

    ConnectionPoolConfig cfg = new ConnectionPoolConfig();
    JedisPooled pooled = new JedisPooled(cfg, hp.getHost(), hp.getPort());

    String key = "jedis:pipe:key";
    String cntKey = "jedis:pipe:cnt";
    pooled.del(key, cntKey);

    Pipeline p = pooled.pipelined();

    Response<String> pong = p.ping();
    Response<String> setOk = p.set(key, "v");
    Response<Long> cnt = p.incr(cntKey);
    Response<String> val = p.get(key);

    p.sync();

    assertEquals("PONG", pong.get());
    assertEquals("OK", setOk.get().toUpperCase(Locale.ROOT));
    assertEquals(1L, cnt.get());
    assertEquals("v", val.get());

    pooled.close();
  }
}
