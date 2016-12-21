package me.nielcho;

import me.nielcho.proto.PersonProto;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
public class ProtoBufRedisApplicationTests {

    @Autowired
    RedisTemplate<Object, Object> redisTemplate;

    @Test
    public void contextLoads() {
    }

    @Test
    public void test() {
        String email = "danielnanuk@gmail.com";
        String name = "nielcho";
        String key = "nielcho";
        PersonProto.Person person0 = PersonProto.Person.newBuilder().setId(1).setName(name).setEmail(email).build();
        redisTemplate.opsForValue().set(key, person0);
        PersonProto.Person person1 = (PersonProto.Person) redisTemplate.opsForValue().get(key);
        Assert.assertEquals(person1.getEmail(), email);
    }

}
