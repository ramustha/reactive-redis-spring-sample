package com.example.reactiveredis.reactiveredis;

import java.util.Arrays;
import lombok.extern.java.Log;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Range;
import org.springframework.data.geo.Metrics;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.core.DefaultTypedTuple;
import org.springframework.data.redis.core.ReactiveGeoOperations;
import org.springframework.data.redis.core.ReactiveHashOperations;
import org.springframework.data.redis.core.ReactiveListOperations;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.ReactiveSetOperations;
import org.springframework.data.redis.core.ReactiveZSetOperations;
import org.springframework.stereotype.Component;

@Log
@Component
public class SampleOps {

  @Autowired
  public void opsForList(ReactiveRedisTemplate<Object, Object> aTemplate) {
    ReactiveListOperations<Object, Object> opsForList = aTemplate.opsForList();

    // add to left list
    opsForList.leftPush("keyList1", "valueList1").subscribe(aRes -> log.info("leftPush: " + aRes));
    opsForList.leftPushAll("keyList2", "valueList2", "valueList21", "valueList22").subscribe(aRes -> log.info("leftPushAll: " + aRes));

    // remove and get to left list
    opsForList.leftPop("keyList1").subscribe(aRes -> log.info("leftPop: " + aRes));
    opsForList.leftPop("keyList2").subscribe(aRes -> log.info("leftPop: " + aRes));

    // add to right list
    opsForList.rightPushAll("keyList3", "valueList3", "valueList31").subscribe(aRes -> log.info("rightPushAll: " + aRes));
    opsForList.rightPushAll("keyList4", "valueList4", "valueList41", "valueList42").subscribe(aRes -> log.info("rightPushAll: " + aRes));

    // remove and get to right list
    opsForList.rightPop("keyList3").subscribe(aRes -> log.info("rightPop: " + aRes));
    opsForList.rightPop("keyList4").subscribe(aRes -> log.info("rightPop: " + aRes));

    // to last length
    opsForList.range("keyList2", 0, -1).subscribe(aRes -> log.info("range keyList2: " + aRes));
    opsForList.range("keyList3", 0, -1).subscribe(aRes -> log.info("range keyList3: " + aRes));

    opsForList.size("keyList2").subscribe(aRes -> log.info("size keyList2: " + aRes));
    opsForList.size("keyList3").subscribe(aRes -> log.info("size keyList3: " + aRes));

    opsForList.delete("keyList1").subscribe(aRes -> log.info("delete keyList1: " + aRes));
    opsForList.delete("keyList2").subscribe(aRes -> log.info("delete keyList2: " + aRes));
    opsForList.delete("keyList3").subscribe(aRes -> log.info("delete keyList3: " + aRes));
    opsForList.delete("keyList4").subscribe(aRes -> log.info("delete keyList4: " + aRes));

    opsForList.size("keyList1").subscribe(aRes -> log.info("size keyList1: " + aRes));
    opsForList.size("keyList2").subscribe(aRes -> log.info("size keyList2: " + aRes));
    opsForList.size("keyList3").subscribe(aRes -> log.info("size keyList3: " + aRes));
    opsForList.size("keyList4").subscribe(aRes -> log.info("size keyList4: " + aRes));
  }

  @Autowired
  public void opsForSets(ReactiveRedisTemplate<Object, Object> aTemplate) {
    ReactiveSetOperations<Object, Object> opsForSet = aTemplate.opsForSet();

    // add to set
    opsForSet.add("keySets1", "valueSets1").subscribe(aRes -> log.info("add: " + aRes));
    opsForSet.add("keySets1", "valueSets2").subscribe(aRes -> log.info("add: " + aRes));
    opsForSet.add("keySets1", "valueSets3").subscribe(aRes -> log.info("add: " + aRes));
    opsForSet.add("keySets2", "valueSets2", "valueSets21").subscribe(aRes -> log.info("adds: " + aRes));

    // get last set
    opsForSet.scan("keySets1").subscribe(aRes -> log.info("scan keySet1: " + aRes));
    opsForSet.scan("keySets2").subscribe(aRes -> log.info("scan keySet2: " + aRes));

    // remove and get last set
    opsForSet.pop("keySets1").subscribe(aRes -> log.info("pop keySet1: " + aRes));
    opsForSet.pop("keySets2").subscribe(aRes -> log.info("pop keySet2: " + aRes));

    opsForSet.members("keySets1").subscribe(aRes -> log.info("members keySet1: " + aRes));
    opsForSet.members("keySets2").subscribe(aRes -> log.info("members keySet2: " + aRes));
    opsForSet.members("keySets3").subscribe(aRes -> log.info("members keySet3: " + aRes));

    opsForSet.isMember("keySets1", "valueSets1").subscribe(aRes -> log.info("ismembers keySet1: " + aRes));
    opsForSet.isMember("keySets2", "valueSets2").subscribe(aRes -> log.info("ismembers keySet2: " + aRes));
    opsForSet.isMember("keySets2", "valueSets21").subscribe(aRes -> log.info("ismembers keySet2: " + aRes));
    opsForSet.isMember("keySets3", "valueSets21").subscribe(aRes -> log.info("ismembers keySet3: " + aRes));

    opsForSet.size("keySets1").subscribe(aRes -> log.info("card keySet1: " + aRes));
    opsForSet.size("keySets2").subscribe(aRes -> log.info("card keySet2: " + aRes));
    opsForSet.size("keySets3").subscribe(aRes -> log.info("card keySet3: " + aRes));
  }

  @Autowired
  public void opsForZSets(ReactiveRedisTemplate<Object, Object> aTemplate) {
    ReactiveZSetOperations<Object, Object> opsForZSet = aTemplate.opsForZSet();

    // add to set
    opsForZSet.add("keyZSets1", "valueZSets1", 1).subscribe(aRes -> log.info("add Z keyZSets1: " + aRes));
    opsForZSet.add("keyZSets2", "valueZSets2", 2).subscribe(aRes -> log.info("add Z keyZSets2: " + aRes));
    opsForZSet.addAll("keyZSets1", Arrays.asList(
        new DefaultTypedTuple<>("valueZSets2", 2D),
        new DefaultTypedTuple<>("valueZSets3", 3D),
        new DefaultTypedTuple<>("valueZSets3", 5D),
        new DefaultTypedTuple<>("valueZSets4", 4D)
    )).subscribe(aRes -> log.info("adds Z keyZSets1: " + aRes));

    opsForZSet.size("keyZSets1").subscribe(aRes -> log.info("card keyZSets1: " + aRes));
    opsForZSet.size("keyZSets2").subscribe(aRes -> log.info("card keyZSets2: " + aRes));

    opsForZSet.score("keyZSets1", "valueZSets1").subscribe(aRes -> log.info("score keyZSets1 valueZSets1: " + aRes));
    opsForZSet.score("keyZSets1", "valueZSets3").subscribe(aRes -> log.info("score keyZSets1 valueZSets3: " + aRes));
    opsForZSet.score("keyZSets2", "valueZSets2").subscribe(aRes -> log.info("score keyZSets2 valueZSets2: " + aRes));

    Range.Bound<Long> lowerInclusive = Range.Bound.inclusive(0L);
    Range.Bound<Long> upperInclusive = Range.Bound.inclusive(2L);
    opsForZSet.rangeWithScores("keyZSets1", Range.of(lowerInclusive, upperInclusive))
        .subscribe(aRes -> log.info("rangeWithScores Inclusive: " + aRes.getValue() + " score: " + aRes.getScore()));

    Range.Bound<Long> lowerExclusive = Range.Bound.exclusive(0L);
    Range.Bound<Long> upperExclusive = Range.Bound.inclusive(2L);
    opsForZSet.rangeWithScores("keyZSets1", Range.of(lowerExclusive, upperExclusive))
        .subscribe(aRes -> log.info("rangeWithScores Exclusive: " + aRes.getValue() + " score: " + aRes.getScore()));

    Range.Bound<Double> lowerInclusive2 = Range.Bound.inclusive(0D);
    Range.Bound<Double> upperInclusive2 = Range.Bound.inclusive(2D);
    opsForZSet.rangeByScore("keyZSets1", Range.of(lowerInclusive2, upperInclusive2))
        .subscribe(aRes -> log.info("rangeByScore Inclusive2: " + aRes));

    Range.Bound<Double> lowerExclusive2 = Range.Bound.exclusive(0D);
    Range.Bound<Double> upperExclusive2 = Range.Bound.exclusive(2D);
    opsForZSet.rangeByScore("keyZSets1", Range.of(lowerExclusive2, upperExclusive2))
        .subscribe(aRes -> log.info("rangeByScore Exclusive2: " + aRes));
  }

  @Autowired
  public void opsForHash(ReactiveRedisTemplate<Object, Object> aTemplate) {
    ReactiveHashOperations<Object, Object, Object> opsForHash = aTemplate.opsForHash();

    opsForHash.put("keyHash1", "hashKey1", "valueHash1").subscribe(aRes -> log.info("put keyHash1: " + aRes));
    // update keyHash1
    opsForHash.put("keyHash1", "hashKey1", "valueHash12").subscribe(aRes -> log.info("put keyHash1: " + aRes));
    opsForHash.put("keyHash1", "hashKey2", "valueHash13").subscribe(aRes -> log.info("put keyHash1: " + aRes));
    opsForHash.put("keyHash2", "hashKey2", "valueHash2").subscribe(aRes -> log.info("put keyHash2: " + aRes));

    opsForHash.get("keyHash1", "hashKey1").subscribe(aRes -> log.info("get keyHash1: " + aRes));
    opsForHash.get("keyHash1", "hashKey2").subscribe(aRes -> log.info("get keyHash1: " + aRes));
    opsForHash.get("keyHash2", "hashKey1").subscribe(aRes -> log.info("get keyHash2: " + aRes));
    opsForHash.get("keyHash2", "hashKey2").subscribe(aRes -> log.info("get keyHash2: " + aRes));

    opsForHash.size("keyHash1").subscribe(aRes -> log.info("size keyHash1: " + aRes));
    opsForHash.size("keyHash2").subscribe(aRes -> log.info("size keyHash2: " + aRes));

    opsForHash.keys("keyHash1").subscribe(aRes -> log.info("keys keyHash1: " + aRes));
    opsForHash.keys("keyHash2").subscribe(aRes -> log.info("keys keyHash2: " + aRes));

    opsForHash.values("keyHash1").subscribe(aRes -> log.info("values keyHash1: " + aRes));
    opsForHash.values("keyHash2").subscribe(aRes -> log.info("values keyHash2: " + aRes));

    //get ALL
    opsForHash.entries("keyHash1").subscribe(aRes -> log.info("entries keyHash1: " + aRes.getKey() + " value " + aRes.getValue()));
    opsForHash.entries("keyHash2").subscribe(aRes -> log.info("entries keyHash2: " + aRes.getKey() + " value " + aRes.getValue()));
  }

  @Autowired
  public void opsForGeo(ReactiveRedisTemplate<Object, Object> aTemplate) {
    ReactiveGeoOperations<Object, Object> opsForGeo = aTemplate.opsForGeo();

    opsForGeo.add("keyGeo1", new Point(107.619125, -6.917464), "Bandung").subscribe(aRes -> log.info("add keyGeo1: " + aRes));
    opsForGeo.add("keyGeo1", new Point(106.865036, -6.175110), "Jakarta").subscribe(aRes -> log.info("add keyGeo1: " + aRes));

    opsForGeo.position("keyGeo1", "Bandung").subscribe(aRes -> log.info("position Bandung: " + aRes));
    opsForGeo.position("keyGeo1", "Jakarta").subscribe(aRes -> log.info("position Jakarta: " + aRes));

    opsForGeo.distance("keyGeo1", "Bandung", "Jakarta", Metrics.KILOMETERS).subscribe(aRes -> log.info("distance Bandung-Jakarta: " + aRes));

    opsForGeo.hash("keyGeo1", "Bandung", "Jakarta").subscribe(aRes -> log.info("hash Bandung Jakarta: " + aRes));
  }
}
