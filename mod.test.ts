import { assert, assertEquals, assertRejects } from "https://deno.land/std@0.160.0/testing/asserts.ts";
import { RateLimiter, TaskBase, TaskManager } from "./mod.ts";

function wait(ms: number): Promise<void> {
    return new Promise<void>(resolve => setTimeout(() => resolve(), ms));
    
}

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

Deno.test({
    name: "TaskManager rate limiting",
    fn: async () => {
        const outstandingTasks: TaskBase[] = [];
        const rateLimit = { count: 3, periodMS: 1000 };
        const onRunTask = (t: TaskBase) => {
            outstandingTasks.splice(outstandingTasks.indexOf(t), 1);
            return Promise.resolve(t.id);
        };

        let tm = new TaskManager<TaskBase, string>({ rateLimit, onRunTask });

        function runTask(s: string, time: Date) {
            const t = { id: s};
            outstandingTasks.push(t);
            return tm.run(t, time);
        }

        const now = new Date();
        tm.start(now);
        runTask("a", now);
        runTask("b", now);
        const promiseC = runTask("c", now);
        assertEquals(runTask("d", now), null);
        runTask("e", now);

        // a, b, c should have run
        assertEquals(outstandingTasks.length, 2);
        assertEquals(outstandingTasks.findIndex(t => t.id === "d"), 0);
        assertEquals(outstandingTasks.findIndex(t => t.id === "e"), 1);

        // Round-trip through JSON
        assertEquals(await promiseC, "c");
        tm.stop();
        tm = TaskManager.deserialize<TaskBase, string>(tm.serialize(), { onRunTask });
        tm.start(now);

        assertEquals(outstandingTasks.length, 2);
        assertEquals(outstandingTasks.findIndex(t => t.id === "d"), 0);
        assertEquals(outstandingTasks.findIndex(t => t.id === "e"), 1);

        // Advance time
        tm.stop();
        const later = new Date(now.valueOf() + 1000 + 1);
        tm.start(later);

        // All should be done
        assertEquals(outstandingTasks.length, 0);
    },
});

Deno.test({
    name: "TaskManager coalescing",
    fn: async () => {
        let runCount = 0;
        const onRunTask = () => {
            ++runCount;
            return Promise.resolve();
        };

        let tm = new TaskManager<TaskBase, void>({
            rateLimit: { count: 1, periodMS: 1000 },
            onRunTask,
        });

        const now = new Date();
        tm.start(now);
        const p = tm.run({ id: "c" }, now); // One to fill up the rate limit
        tm.run({ id: "a" }, now);
        tm.run({ id: "a" }, now); // Duplicate; should be coalesced
        tm.run({ id: "b" }, now);
        assertEquals(runCount, 1);

        // Need to let task completion handlers run
        await p;

        // Override the rate limit
        tm.stop();
        tm = TaskManager.deserialize<TaskBase, void>(tm.serialize(), {
            rateLimit: { count: 5, periodMS: 1000 },
            onRunTask,
        });

        tm.start(new Date(now.valueOf() + 5000));
        assertEquals(runCount, 3);
    },
});

Deno.test({
    name: "TaskManager failure handler",
    fn: async () => {
        let failed = false;
        const promise = Promise.reject();
        const tm = new TaskManager<TaskBase, void>({
            rateLimit: { count: 10, periodMS: 1000 },
            onRunTask: () => promise,
            onTaskFailure: () => { failed = true; },
        });

        tm.start();
        tm.run({ id: "a" });
        await assertRejects(() => promise);
        assertEquals(failed, true);
    },
});

Deno.test({
    name: "TaskManager in-progress task serialization",
    fn: async () => {
        let startCount = 0;
        let endCount = 0;
        let done = false;
        const promise = (async () => {
            while (!done) {
                await wait(25);
            }
        })();

        const onRunTask = async () => {
            ++startCount;
            await promise;
            ++endCount;
        };

        let tm = new TaskManager<TaskBase, void>({
            rateLimit: { count: 10, periodMS: 1000 },
            onRunTask,
        });

        tm.start();
        tm.run({ id: "a" });
        assertEquals(startCount, 1);
        assertEquals(endCount, 0);
        tm.stop();

        tm = TaskManager.deserialize<TaskBase, void>(tm.serialize(), { onRunTask });

        // Task should be started a second time
        tm.start();
        assertEquals(startCount, 2);
        assertEquals(endCount, 0);

        done = true;
        await promise;
        assertEquals(startCount, 2);
        assertEquals(endCount, 2);
    },
});