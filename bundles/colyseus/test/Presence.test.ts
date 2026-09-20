import assert from "assert";
import { LocalPresence, type Presence, RedisPresence } from "../src/index.ts";
import { timeout } from "./utils/index.ts";

const PRESENCE_IMPLEMENTATIONS = [LocalPresence, RedisPresence];

describe("Presence", () => {

  for (let i = 0; i < PRESENCE_IMPLEMENTATIONS.length; i++) {
    let presence: Presence;

    describe(`Presence:${(PRESENCE_IMPLEMENTATIONS[i]).name}`, () => {
      beforeEach(() => presence = new PRESENCE_IMPLEMENTATIONS[i]())
      afterEach(() => presence.shutdown());

      it("subscribe", async () => {
        let i = 0;

        await presence.subscribe("topic", (data) => {
          if (i === 0) {
            assert.equal("string", data);

          } else if (i === 1) {
            assert.equal(1000, data);

          } else if (i === 2) {
            assert.deepEqual({ object: "hello world" }, data);
          }

          i++;

          if (i === 3) {
            presence.unsubscribe("topic");
          }
        });

        await presence.publish("topic", "string");
        await presence.publish("topic", 1000);
        await presence.publish("topic", { object: "hello world" });

        await timeout(10);

        assert.equal(i, 3);
      });

      it("subscribe: multiple callbacks for same topic", async () => {
        let messages: any[] = [];
        const callback1 = (data) => messages.push(data);
        const callback2 = (data) => messages.push(data);
        const callback3 = (data) => messages.push(data);

        await presence.subscribe("topic-multi", callback1);
        await presence.subscribe("topic-multi", callback2);
        await presence.subscribe("topic-multi", callback3);
        await presence.publish("topic-multi", 1);

        await timeout(10);

        assert.deepEqual([1, 1, 1], messages);

        await presence.unsubscribe("topic-multi", callback1);
        await presence.publish("topic-multi", 1);

        await timeout(10);

        assert.deepEqual([1, 1, 1, 1, 1], messages);
      })

      it("subscribe: topics should not collide", async () => {
        let messages: any[] = [];
        const callback1 = (data) => messages.push(data);
        const callback2 = (data) => messages.push(data);
        const callback3 = (data) => messages.push(data);
        const callback4 = (data) => messages.push(data);

        // subscribe to each topic twice
        await presence.subscribe("topic-collide1", callback1);
        await presence.subscribe("topic-collide1", callback2);
        await presence.subscribe("topic-collide2", callback3);
        await presence.subscribe("topic-collide2", callback4);

        await presence.publish("topic-collide1", 1);
        await presence.publish("topic-collide1", 2);
        await presence.publish("topic-collide2", 3);
        await presence.publish("topic-collide2", 4);

        await timeout(10);
        assert.deepEqual([1, 1, 2, 2, 3, 3, 4, 4], messages);

        // leave duplicated subscriptions
        await presence.unsubscribe("topic-collide1", callback2);
        await presence.unsubscribe("topic-collide2", callback4);

        messages = [];
        await presence.publish("topic-collide1", 1);
        await presence.publish("topic-collide1", 2);
        await presence.publish("topic-collide2", 3);
        await presence.publish("topic-collide2", 4);

        await timeout(10);
        assert.deepEqual([1, 2, 3, 4], messages);

        // leave all subscriptions...
        assert.ok(presence['subscriptions'].listenerCount("topic-collide1") > 0);
        assert.ok(presence['subscriptions'].listenerCount("topic-collide2") > 0);
        await presence.unsubscribe("topic-collide1", callback1);
        await presence.unsubscribe("topic-collide2", callback3);
        assert.strictEqual(0, presence['subscriptions'].listenerCount("topic-collide1"));
        assert.strictEqual(0, presence['subscriptions'].listenerCount("topic-collide2"));

        messages = [];
        await presence.publish("topic-collide1", 1000);
        await presence.publish("topic-collide2", 2000);

        await timeout(10);
        assert.deepEqual([], messages);
      });

      it("unsubscribe", async () => {
        presence.subscribe("topic2", (_) => assert.fail("should not trigger"));
        presence.unsubscribe("topic2");
        presence.publish("topic2", "hello world!");
        assert.ok(true);
      });

      it("unsubscribe from non-existing callback", async () => {
        let callCount = 0;
        await presence.subscribe("topic", (_) => { callCount++; });
        await presence.unsubscribe("topic", function() {});
        await presence.publish("topic", "hello world!");
        await timeout(10);
        assert.strictEqual(1, callCount);
      });

      it("unsubscribe while triggering", async () => {
        const topic = "unsubscribe-ongoing";

        let calls: string[] = [];

        const one = (_) => calls.push("one");
        const two = (_) => calls.push("two");
        const three = (_) => calls.push("three");
        const four = (_) => calls.push("four");

        await presence.subscribe(topic, one);
        await presence.subscribe(topic, two);
        await presence.subscribe(topic, async () => {
          await presence.unsubscribe(topic, four);

        });
        await presence.subscribe(topic, three);
        await presence.subscribe(topic, four);

        await presence.publish(topic, {});
        await timeout(10);

        assert.deepStrictEqual(["one", "two", "three", "four"], calls);
      });

      it("exists", async () => {
        await presence.set("exists1", "hello world");
        assert.equal(true, await presence.exists("exists1"));
        assert.equal(false, await presence.exists("exists2"));

        await presence.del("exists1");
        assert.equal(false, await presence.exists("exists1"));
      });

      it("set", async () => {
        await presence.set("setval1", "hello world");
        assert.equal("hello world", await presence.get("setval1"));

        await presence.del("setval1");
        assert.equal(undefined, await presence.get("setval1"));
      });

      it("setex", async () => {
        await presence.setex("setex1", "hello world", 1);
        assert.equal("hello world", await presence.get("setex1"));

        await timeout(1100);
        assert.ok(!(await presence.get("setex1")));
      });

      describe("expire", () => {
        it("expire should remove a set created with sadd", async () => {
          await presence.sadd("expire-set", "one");
          await presence.sadd("expire-set", "two");
          await presence.expire("expire-set", 0.1);

          assert.deepEqual(["one", "two"], await presence.smembers("expire-set"));
          assert.equal(true, await presence.exists("expire-set"));

          await timeout(250);

          assert.deepEqual([], await presence.smembers("expire-set"));
          assert.equal(0, await presence.scard("expire-set"));
          assert.equal(0, await presence.sismember("expire-set", "one"));
          assert.equal(false, await presence.exists("expire-set"));
        });

        it("expire should remove a hash created with hset", async () => {
          await presence.hset("expire-hash", "one", "1");
          await presence.hset("expire-hash", "two", "2");
          await presence.expire("expire-hash", 0.1);

          assert.equal("1", await presence.hget("expire-hash", "one"));
          assert.deepEqual({ one: "1", two: "2" }, await presence.hgetall("expire-hash"));
          assert.equal(true, await presence.exists("expire-hash"));

          await timeout(250);

          assert.equal(null, await presence.hget("expire-hash", "one"));
          assert.deepEqual({}, await presence.hgetall("expire-hash"));
          assert.equal(0, await presence.hlen("expire-hash"));
          assert.equal(false, await presence.exists("expire-hash"));
        });

        it("expire on a string should remove the key", async () => {
          await presence.set("expire-string", "value");
          await presence.expire("expire-string", 0.1);

          assert.equal("value", await presence.get("expire-string"));

          await timeout(250);

          assert.equal(null, await presence.get("expire-string"));
          assert.equal(false, await presence.exists("expire-string"));
        });

        it("re-expire should extend the key's lifetime", async () => {
          await presence.setex("re-expire", "value", 0.3);
          await timeout(200);

          // still alive, extend for another 0.3s
          await presence.expire("re-expire", 0.3);
          await timeout(200);
          assert.equal("value", await presence.get("re-expire"));

          await timeout(200);
          assert.equal(null, await presence.get("re-expire"));
        });

        it("set before TTL elapses should keep the rewritten value", async () => {
          await presence.setex("rewrite-string", "old", 0.1);
          await timeout(50);

          // plain SET rewrites the value and clears the TTL
          await presence.set("rewrite-string", "new");

          await timeout(250);
          assert.equal("new", await presence.get("rewrite-string"));
          assert.equal(true, await presence.exists("rewrite-string"));

          await presence.del("rewrite-string");
        });

        it("setex before TTL elapses should follow the new TTL", async () => {
          await presence.setex("rewrite-setex", "old", 0.3);
          await timeout(50);

          // rewrite with a fresh, shorter lifecycle
          await presence.setex("rewrite-setex", "new", 0.1);
          await timeout(50);
          assert.equal("new", await presence.get("rewrite-setex"));

          // the old (0.3s) lifecycle must not have deleted the new value...
          await timeout(350);
          assert.equal(null, await presence.get("rewrite-setex"));
        });

        it("del before TTL elapses, then re-create, should not be deleted by the old timeout", async () => {
          await presence.setex("rewrite-del", "old", 0.1);
          await timeout(50);
          await presence.del("rewrite-del");

          await presence.set("rewrite-del", "new");
          await timeout(250);
          assert.equal("new", await presence.get("rewrite-del"));

          await presence.del("rewrite-del");
        });
      });

      describe("mixed key types sharing the same name", () => {
        it("string, set and hash lifecycles must not leak across types", async () => {
          const key = "mixed-key";

          // string lifecycle expires
          await presence.setex(key, "string-value", 0.1);
          assert.equal("string-value", await presence.get(key));
          await timeout(250);
          assert.equal(false, await presence.exists(key));
          assert.equal(null, await presence.get(key));

          // same name, now a set: old string timeout must not delete it
          await presence.sadd(key, "member");
          assert.equal(true, await presence.exists(key));
          assert.deepEqual(["member"], await presence.smembers(key));
          await timeout(250);
          assert.deepEqual(["member"], await presence.smembers(key));

          // give the set its own TTL
          await presence.expire(key, 0.1);
          await timeout(250);
          assert.equal(false, await presence.exists(key));
          assert.deepEqual([], await presence.smembers(key));

          // same name, now a hash: old set timeout must not delete it
          await presence.hset(key, "field", "hash-value");
          assert.equal(true, await presence.exists(key));
          assert.equal("hash-value", await presence.hget(key, "field"));
          await timeout(250);
          assert.equal("hash-value", await presence.hget(key, "field"));

          await presence.expire(key, 0.1);
          await timeout(250);
          assert.equal(false, await presence.exists(key));
          assert.deepEqual({}, await presence.hgetall(key));

          // back to a string: old hash timeout must not delete it
          await presence.setex(key, "again", 0.3);
          await timeout(50);
          await presence.set(key, "persisted");
          await timeout(400);
          assert.equal("persisted", await presence.get(key));

          await presence.del(key);
          assert.equal(false, await presence.exists(key));
        });
      });

      it("get", async () => {
        await presence.setex("setex2", "one", 1);
        assert.equal("one", await presence.get("setex2"));

        await presence.setex("setex3", "two", 1);
        assert.equal("two", await presence.get("setex3"));
      });

      it("del", async () => {
        await presence.setex("setex4", "one", 1);
        await presence.del("setex4");
        assert.ok(!(await presence.get("setex4")));
      });

      it("sadd/smembers/srem (sets)", async () => {
        await presence.sadd("set", 1);
        await presence.sadd("set", 2);
        await presence.sadd("set", 3);
        assert.deepEqual([1, 2, 3], await presence.smembers("set"));
        assert.equal(3, await presence.scard("set"));

        await presence.srem("set", 2);
        assert.deepEqual([1, 3], await presence.smembers("set"));
        assert.equal(2, await presence.scard("set"));

        await presence.del("set");
        assert.equal(0, await presence.scard("set"));
      });

      it("sismember", async () => {
        await presence.sadd("sis", "testvalue");
        await presence.sadd("sis", "anothervalue");
        assert.equal(1, await presence.sismember("sis", "testvalue"));
        assert.equal(1, await presence.sismember("sis", "anothervalue"));
        assert.equal(0, await presence.sismember("sis", "notexistskey"));
      });

      it("sinter - intersection between sets", async () => {
        await presence.sadd("key1", "a");
        await presence.sadd("key1", "b");
        await presence.sadd("key1", "c");
        await presence.sadd("key2", "c");
        await presence.sadd("key2", "d");
        await presence.sadd("key2", "e");

        const intersection = await presence.sinter("key1", "key2");
        assert.deepEqual(["c"], intersection);
      });

      it("hset/hget/hdel/hlen (hashes)", async () => {
        await presence.hset("hash", "one", "1");
        await presence.hset("hash", "two", "2");
        await presence.hset("hash", "three", "3");

        assert.equal(3, await presence.hlen("hash"));
        assert.equal("1", await presence.hget("hash", "one"));
        assert.equal("2", await presence.hget("hash", "two"));
        assert.equal("3", await presence.hget("hash", "three"));
        assert.ok(!(await presence.hget("hash", "four")));

        const hdelSuccess = await presence.hdel("hash", "two");
        assert.equal(true, hdelSuccess);
        assert.equal(2, await presence.hlen("hash"));
        assert.ok(!(await presence.hget("hash", "two")));

        const hdelFailure = await presence.hdel("none", "none");
        assert.equal(false, hdelFailure);
      });

      it("incr", async () => {
        await presence.del("num"); //ensure key doens't exist before testing

        var incr: number;

        incr = await presence.incr("num");
        assert.strictEqual(1, incr);

        incr = await presence.incr("num");
        assert.strictEqual(2, incr);

        incr = await presence.incr("num");
        assert.strictEqual(3, incr);

        assert.equal(3, await presence.get("num"));
      });

      it("decr", async () => {
        await presence.del("num"); //ensure key doens't exist before testing

        var decr: number;

        decr = await presence.decr("num");
        assert.strictEqual(-1, decr);

        decr = await presence.decr("num");
        assert.strictEqual(-2, decr);

        decr = await presence.decr("num");
        assert.strictEqual(-3, decr);

        assert.equal(-3, await presence.get("num"));
      });

      it("hincrby", async () => {
        await presence.del("hincrby"); //ensure key doens't exist before testing

        var hincrby: number;

        hincrby = await presence.hincrby("hincrby", "one", 1);
        assert.strictEqual(1, hincrby);

        hincrby = await presence.hincrby("hincrby", "one", 1);
        assert.strictEqual(2, hincrby);

        hincrby = await presence.hincrby("hincrby", "one", 1);
        assert.strictEqual(3, hincrby);

        assert.strictEqual('3', await presence.hget("hincrby", "one"));
      });

      it("prototype names are plain keys (#942)", async () => {
        assert.strictEqual(false, await presence.exists("constructor"));
        assert.strictEqual(null, await presence.hget("toString", "x"));
        assert.deepStrictEqual([], await presence.smembers("constructor"));

        await presence.hset("__proto__", "polluted", "yes");
        assert.strictEqual(undefined, ({} as any).polluted);
        assert.strictEqual("yes", await presence.hget("__proto__", "polluted"));

        await presence.hincrby("__proto__", "constructor", 1);
        assert.strictEqual("1", await presence.hget("__proto__", "constructor"));

        // both the set name and the member
        await presence.sadd("constructor", "constructor");
        await presence.sadd("toString", "constructor");
        assert.deepStrictEqual(["constructor"], await presence.smembers("constructor"));
        assert.deepStrictEqual(["constructor"], await presence.sinter("constructor", "toString"));

        await presence.del("__proto__");
        await presence.del("constructor");
        await presence.del("toString");
        assert.strictEqual(false, await presence.exists("__proto__"));
      });

      it("channels", async () => {
        await presence.subscribe("p:one", () => {});
        await presence.subscribe("$one", () => {});
        await presence.subscribe("p:two", () => {});
        await presence.subscribe("$two", () => {});
        await presence.subscribe("one.two", () => {});

        const channels = await presence.channels();
        assert.deepStrictEqual(["p:one", "$one", "p:two", "$two", "one.two"].sort(), channels.sort());

        const pChannels = await presence.channels("p:*");
        assert.deepStrictEqual(["p:one", "p:two"], pChannels.sort());

        const $Channels = await presence.channels("$*");
        assert.deepStrictEqual(["$one", "$two"], $Channels.sort());

        const dotChannels = await presence.channels("*.*");
        assert.deepStrictEqual(["one.two"], dotChannels.sort());
      });

      describe("brpop", () => {
        // the key outlives the connection: a leftover item lets the blocking
        // test pop an old one instead of waiting for a new one
        beforeEach(() => presence.del("brpop"));

        it("brpop should return existing item", async () => {
          await presence.lpush("brpop", "one", "two", "three");
          const result = await presence.brpop("brpop", 1);
          assert.deepStrictEqual(["brpop", "one"], result);
        });

        it("brpop should return new item", async () => {
          let result: string[] | null = null;
          presence.brpop("brpop", 1).then((r) => {
            result = r;
          }).catch((e) => {
            result = null;
          });

          await presence.lpush("brpop", "one", "two", "three");

          await timeout(200);
          assert.deepStrictEqual(["brpop", "one"], result);
        });

        it("brpop should return null if no item is available", async () => {
          const result = await presence.brpop("none", 0.1);
          assert.deepStrictEqual(null, result);
        });

      });

      describe("hincrbyex", () => {
        it("hincrbyex should increment the value", async () => {
          const value1 = await presence.hincrbyex("hincrbyex", "one", 1, 1);
          assert.strictEqual(1, value1);
          assert.strictEqual("1", await presence.hget("hincrbyex", "one"));
        });

        it("hincrbyex should expire the key after the given time", async () => {
          await presence.hincrbyex("hincrbyex", "expired", 1, 1);
          assert.strictEqual("1", await presence.hget("hincrbyex", "expired"));
          await timeout(1200);
          assert.strictEqual(null, await presence.hget("hincrbyex", "expired"));
        });

      });

      // LocalPresence keeps in-memory timers; these cover timer bookkeeping
      // that the Redis server handles natively.
      if (PRESENCE_IMPLEMENTATIONS[i] === LocalPresence) {
        describe("LocalPresence: TTL lifecycle bookkeeping", () => {
          it("sadd onto an expiring set should keep the set's TTL (Redis parity)", async () => {
            presence.sadd("rewrite-set", "one");
            presence.expire("rewrite-set", 0.2);

            await timeout(50);
            // adding a member to an *existing* set does not reset its TTL
            presence.sadd("rewrite-set", "two");

            await timeout(100);
            assert.deepEqual(["one", "two"], await presence.smembers("rewrite-set"));

            // original TTL (t≈200ms) removes the whole set
            await timeout(150);
            assert.deepEqual([], await presence.smembers("rewrite-set"));
            assert.strictEqual(false, await presence.exists("rewrite-set"));
          });

          it("sadd after del+re-create should not be deleted by the old timeout", async () => {
            presence.sadd("rewrite-set2", "one");
            presence.expire("rewrite-set2", 0.1);
            await timeout(50);
            presence.del("rewrite-set2");

            // same name, newly created set: the old 0.1s timeout must not fire on it
            presence.sadd("rewrite-set2", "two");
            await timeout(200);
            assert.deepEqual(["two"], await presence.smembers("rewrite-set2"));
            assert.strictEqual(true, await presence.exists("rewrite-set2"));

            presence.del("rewrite-set2");
          });

          it("hset onto an expiring hash should keep the hash's TTL (Redis parity)", async () => {
            await presence.hset("rewrite-hash", "one", "1");
            presence.expire("rewrite-hash", 0.2);

            await timeout(50);
            // adding a field to an *existing* hash does not reset its TTL
            await presence.hset("rewrite-hash", "two", "2");

            await timeout(100);
            assert.strictEqual("1", await presence.hget("rewrite-hash", "one"));
            assert.strictEqual("2", await presence.hget("rewrite-hash", "two"));

            // original TTL (t≈200ms) removes the whole hash
            await timeout(150);
            assert.strictEqual(null, await presence.hget("rewrite-hash", "one"));
            assert.strictEqual(false, await presence.exists("rewrite-hash"));
          });

          it("hset after del+re-create should not be deleted by the old timeout", async () => {
            await presence.hset("rewrite-hash2", "one", "1");
            presence.expire("rewrite-hash2", 0.1);
            await timeout(50);
            presence.del("rewrite-hash2");

            // same name, newly created hash: the old 0.1s timeout must not fire on it
            await presence.hset("rewrite-hash2", "two", "2");
            await timeout(200);
            assert.strictEqual("2", await presence.hget("rewrite-hash2", "two"));
            assert.strictEqual(true, await presence.exists("rewrite-hash2"));

            await presence.del("rewrite-hash2");
          });

          it("a type change after expire should not be deleted by the old key's timeout", async () => {
            presence.sadd("type-swap", "member");
            presence.expire("type-swap", 0.1);
            await timeout(50);
            presence.del("type-swap");

            // same name, different type, new lifecycle
            await presence.hset("type-swap", "field", "value");
            await timeout(150);
            assert.strictEqual("value", await presence.hget("type-swap", "field"));
            assert.strictEqual(true, await presence.exists("type-swap"));

            await presence.del("type-swap");
            await timeout(150);
            assert.strictEqual(false, await presence.exists("type-swap"));
          });

          it("hincrbyex refresh should follow the latest expiry", async () => {
            await presence.hincrbyex("hincrbyex-refresh", "f", 1, 0.3);
            await timeout(100);
            // -1 + refreshed 0.4s TTL: the original 0.3s timeout (t≈0) is detached
            await presence.hincrbyex("hincrbyex-refresh", "f", -1, 0.4);

            await timeout(350); // t≈450ms: original TTL (t≈300) must have been detached
            assert.strictEqual("0", await presence.hget("hincrbyex-refresh", "f"));
            assert.strictEqual(true, await presence.exists("hincrbyex-refresh"));

            await timeout(200); // t≈650ms: refreshed TTL (t≈100+400) has fired
            assert.strictEqual(null, await presence.hget("hincrbyex-refresh", "f"));
            assert.strictEqual(false, await presence.exists("hincrbyex-refresh"));
          });

          it("expired set/hash leaves no dangling timers", async () => {
            presence.sadd("dangling-set", "a");
            presence.expire("dangling-set", 0.1);
            await presence.hset("dangling-hash", "f", "v");
            presence.expire("dangling-hash", 0.1);

            await timeout(250);

            assert.strictEqual(undefined, (presence as LocalPresence)["timeouts"]["dangling-set"]);
            assert.strictEqual(undefined, (presence as LocalPresence)["timeouts"]["dangling-hash"]);
            assert.strictEqual(undefined, (presence as LocalPresence)["generations"]["dangling-set"]);
            assert.strictEqual(undefined, (presence as LocalPresence)["generations"]["dangling-hash"]);
          });
        });
      }

    });

  }

  // RedisPresence-specific tests live in ./presence/RedisPresence.test.ts

});

