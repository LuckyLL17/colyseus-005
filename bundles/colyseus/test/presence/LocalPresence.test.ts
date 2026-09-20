import assert from "assert";
import { LocalPresence } from "@colyseus/core";

import { timeout } from "../utils/index.ts";

/**
 * LocalPresence-specific semantics. Unlike Redis, LocalPresence can hold a
 * string, a set/list and a hash under the same key name at once — `expire()`,
 * `del()` and `exists()` treat them as a single key, and any write to the key
 * starts a new lifecycle (a TTL scheduled for the previous value must never
 * delete the new one).
 */
describe("LocalPresence", () => {
  let presence: LocalPresence;

  beforeEach(() => presence = new LocalPresence());
  afterEach(() => presence.shutdown());

  describe("expire (key lifecycle)", () => {

    it("should expire every type sharing the same key name", async () => {
      await presence.set("mixed", "string");
      await presence.sadd("mixed", "member");
      await presence.hset("mixed", "field", "value");
      await presence.expire("mixed", 1);

      assert.equal(true, await presence.exists("mixed"));

      await timeout(1100);

      assert.strictEqual(undefined, await presence.get("mixed"));
      assert.deepEqual([], await presence.smembers("mixed"));
      assert.strictEqual(null, await presence.hget("mixed", "field"));
      assert.deepEqual({}, await presence.hgetall("mixed"));
      assert.equal(false, await presence.exists("mixed"));
    });

    it("re-writing a set before the TTL is up starts a new lifecycle", async () => {
      await presence.sadd("rewrite-set", "one");
      await presence.expire("rewrite-set", 1);

      await timeout(500);
      await presence.sadd("rewrite-set", "two");

      await timeout(700); // 1.2s: past the original TTL
      assert.deepEqual(["one", "two"], await presence.smembers("rewrite-set"));
    });

    it("re-writing a hash before the TTL is up starts a new lifecycle", async () => {
      await presence.hset("rewrite-hash", "one", "1");
      await presence.expire("rewrite-hash", 1);

      await timeout(500);
      await presence.hset("rewrite-hash", "two", "2");

      await timeout(700); // 1.2s: past the original TTL
      assert.equal("1", await presence.hget("rewrite-hash", "one"));
      assert.deepEqual({ one: "1", two: "2" }, await presence.hgetall("rewrite-hash"));
    });

    it("re-writing a string before the TTL is up starts a new lifecycle", async () => {
      await presence.setex("rewrite-string", "one", 1);

      await timeout(500);
      await presence.set("rewrite-string", "two");

      await timeout(700); // 1.2s: past the original TTL
      assert.equal("two", await presence.get("rewrite-string"));
    });

    it("writing any type clears the key's pending TTL", async () => {
      await presence.set("cross-type", "string");
      await presence.expire("cross-type", 1);

      await timeout(500);
      await presence.hset("cross-type", "field", "value");

      await timeout(700); // 1.2s: past the original TTL
      assert.equal("string", await presence.get("cross-type"));
      assert.equal("value", await presence.hget("cross-type", "field"));
      assert.equal(true, await presence.exists("cross-type"));
    });

    it("a stale TTL must not delete a key re-incarnated as another type", async () => {
      await presence.setex("reincarnate", "one", 1);
      await presence.del("reincarnate");
      await presence.hset("reincarnate", "field", "value");

      await timeout(1100);
      assert.equal("value", await presence.hget("reincarnate", "field"));
      assert.equal(true, await presence.exists("reincarnate"));
    });

    it("string counters are not affected by other keys expiring", async () => {
      await presence.incr("counter");
      await presence.incr("counter");
      await presence.setex("counter-neighbor", "x", 1);

      await timeout(1100);

      assert.strictEqual(3, await presence.incr("counter"));
      assert.strictEqual(2, await presence.decr("counter"));
      assert.equal(2, await presence.get("counter"));
    });

  });

});
