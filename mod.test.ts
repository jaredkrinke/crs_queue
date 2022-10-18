import { assert, assertEquals } from "https://deno.land/std@0.160.0/testing/asserts.ts";
import { RateLimiter } from "./mod.ts";

Deno.test({
    name: "RateLimiter serialization",
    fn: () => {
        const rl = new RateLimiter({ count: 5, periodMS: 100});
        const rl2 = RateLimiter.deserialize(rl.serialize());
        assertEquals(rl2.serialize(), rl.serialize());
    },
});

Deno.test({
    name: "RateLimiter limiting",
    fn: () => {
        let rl = new RateLimiter({ count: 5, periodMS: 100});

        // Five requests should be allowed
        const now = new Date();
        assertEquals(rl.tryRequest(now), true);
        assertEquals(rl.tryRequest(new Date(now.valueOf() + 5)), true);
        assertEquals(rl.tryRequest(new Date(now.valueOf() + 10)), true);
        assertEquals(rl.tryRequest(new Date(now.valueOf() + 15)), true);
        assertEquals(rl.tryRequest(new Date(now.valueOf() + 20)), true);

        // No more should be allowed
        for (let i = 0; i < 3; i++) {
            assert(rl.tryRequest(new Date(now.valueOf() + 20)) instanceof Date);
        }

        const later = new Date(now.valueOf() + 100 + 1); // +1 to ensure we're past the window
        assertEquals(rl.tryRequest(later), true);

        // Round-trip through JSON part way through
        rl = RateLimiter.deserialize(rl.serialize());

        assert(rl.tryRequest(new Date(later.valueOf() + 4)) instanceof Date);
        assertEquals(rl.tryRequest(new Date(later.valueOf() + 5)), true);

        assertEquals(rl.tryRequest(new Date(later.valueOf() + 10)), true);
        assertEquals(rl.tryRequest(new Date(later.valueOf() + 15)), true);
        assertEquals(rl.tryRequest(new Date(later.valueOf() + 20)), true);

        for (let i = 0; i < 3; i++) {
            assert(rl.tryRequest(new Date(later.valueOf() + 21)) instanceof Date);
        }
    },
});
