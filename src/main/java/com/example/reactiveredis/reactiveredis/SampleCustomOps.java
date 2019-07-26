package com.example.reactiveredis.reactiveredis;

import java.io.Serializable;
import java.util.Date;
import lombok.Value;
import lombok.extern.java.Log;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.annotation.Id;
import org.springframework.data.redis.core.ReactiveListOperations;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.ReactiveSetOperations;
import org.springframework.data.redis.core.ReactiveValueOperations;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.index.Indexed;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;

@Log
@Component
public class SampleCustomOps {

  @Value
  public static class Person implements Serializable {
    @Id
    String id;
    String firstName;
    String lastName;
    @Indexed
    Date when;
  }

  @Autowired
  public void opsForList(ReactiveRedisTemplate<Object, Object> aTemplate) {
    ReactiveListOperations<Object, Object> opsForList = aTemplate.opsForList();

    Flux.just(
        new Person("p001", "Jack", "Ma", new Date()),
        new Person("p001", "Mad", "Dog", new Date()),
        new Person("p001", "Chun", "Low", new Date())
    )
        .flatMap(aP -> opsForList.leftPush(aP.getId(), aP))
        .subscribe(aRes -> log.info("Person List leftPush: " + aRes));

    opsForList.range("p001", 0, -1).subscribe(aRes -> log.info("Person List range: " + aRes));

    opsForList.leftPop("p001").subscribe(aRes -> log.info("Person List leftPop: " + aRes));
    opsForList.rightPop("p001").subscribe(aRes -> log.info("Person List rightPop: " + aRes));

    aTemplate.keys("p001").subscribe(aRes -> log.info("Person List keys: " + aRes));
    aTemplate.scan(ScanOptions.scanOptions().match("p001").build()).subscribe(aRes -> log.info("Person List scan: " + aRes));

    opsForList.delete("p001").subscribe(aRes -> log.info("Person List delete: " + aRes));
    opsForList.size("p001").subscribe(aRes -> log.info("Person List size: " + aRes));
  }

  // no duplicate
  @Autowired
  public void opsForValue(ReactiveRedisTemplate<Object, Object> aTemplate) {
    ReactiveValueOperations<Object, Object> opsForValue = aTemplate.opsForValue();

    Flux.just(
        new Person("V1", "Jack", "Ma", new Date()),
        new Person("V1", "Mad", "Dog", new Date()),
        new Person("V1", "Chun", "Low", new Date()),
        new Person("V2", "Chun", "Low", new Date())
    )
        .flatMap(aP -> opsForValue.set(aP.getId(), aP))
        .subscribe(aRes -> log.info("Person Value set: " + aRes));

    opsForValue.get("V1").subscribe(aRes -> log.info("Person Value get: " + aRes));
    opsForValue.set("V1", new Person("V1", "Jack", "Ma", new Date())).subscribe(aRes -> log.info("Person Value set: " + aRes));
    opsForValue.get("V1").subscribe(aRes -> log.info("Person Value get: " + aRes));

    opsForValue.get("V2").subscribe(aRes -> log.info("Person Value get: " + aRes));

    opsForValue.delete("V1").subscribe(aRes -> log.info("Person Value delete: " + aRes));
    opsForValue.size("V1").subscribe(aRes -> log.info("Person Value size: " + aRes));
  }

  @Autowired
  public void opsForSet(ReactiveRedisTemplate<Object, Object> aTemplate) {
    ReactiveSetOperations<Object, Object> opsForSet = aTemplate.opsForSet();

    Flux.just(
        new Person("S1", "Jack", "Ma", new Date()),
        new Person("S1", "Mad", "Dog", new Date()),
        new Person("S1", "Mad", "Dog", new Date()),
        new Person("S1", "Chun", "Low", new Date()),
        new Person("S2", "Chun", "Low", new Date())
    )
        .flatMap(aP -> opsForSet.add(aP.getId(), aP))
        .subscribe(aRes -> log.info("Person Sets set: " + aRes));

    opsForSet.difference("S1", "S2").subscribe(aRes -> log.info("Person Sets difference: " + aRes));

    opsForSet.members("S1").subscribe(aRes -> log.info("Person Sets members: " + aRes));
    opsForSet.scan("S1").subscribe(aRes -> log.info("Person Sets scan: " + aRes));

    opsForSet.delete("S1").subscribe(aRes -> log.info("Person Sets delete: " + aRes));
    opsForSet.size("S1").subscribe(aRes -> log.info("Person Sets size: " + aRes));
  }
}
