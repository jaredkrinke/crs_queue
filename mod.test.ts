import { assert, assertEquals, assertRejects } from "https://deno.land/std@0.160.0/testing/asserts.ts";
import { RateLimiter, TaskBase, TaskManager } from "./mod.ts";

function wait(ms: number): Promise<void> {
    return new Promise<void>(resolve => setTimeout(() => resolve(), ms));
}

function later(now: Date, delayMS: number): Date {
    return new Date(now.valueOf() + delayMS);
}

// Mock "new Date()"
let now = new Date();
RateLimiter["getNow"] = () => now;
TaskManager["getNow"] = () => now;

Deno.test({
    name: "RateLimiter property serialization",
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
        const originalNow = now;
        RateLimiter["getNow"] = () => now;
        assertEquals(rl.tryRequest(), true);
        now = later(now, 5);
        assertEquals(rl.tryRequest(), true);
        now = later(now, 5);
        assertEquals(rl.tryRequest(), true);
        now = later(now, 5);
        assertEquals(rl.tryRequest(), true);
        now = later(now, 5);
        assertEquals(rl.tryRequest(), true);

        // No more should be allowed
        for (let i = 0; i < 3; i++) {
            assert(rl.tryRequest() instanceof Date);
        }

        now = later(originalNow, 100 + 1); // +1 to ensure we're past the window
        assertEquals(rl.tryRequest(), true);

        // Round-trip through JSON part way through
        rl = RateLimiter.deserialize(rl.serialize());

        now = later(now, 4);
        assert(rl.tryRequest() instanceof Date);
        now = later(now, 1);
        assertEquals(rl.tryRequest(), true);

        now = later(now, 5);
        assertEquals(rl.tryRequest(), true);
        now = later(now, 5);
        assertEquals(rl.tryRequest(), true);
        now = later(now, 5);
        assertEquals(rl.tryRequest(), true);

        now = later(now, 1);
        for (let i = 0; i < 3; i++) {
            assert(rl.tryRequest() instanceof Date);
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

        function runTask(s: string) {
            const t = { id: s};
            outstandingTasks.push(t);
            return tm.run(t);
        }

        runTask("a");
        runTask("b");
        const promiseC = runTask("c");
        assertEquals(runTask("d"), null);
        runTask("e");

        // a, b, c should have run
        assertEquals(outstandingTasks.length, 2);
        assertEquals(outstandingTasks.findIndex(t => t.id === "d"), 0);
        assertEquals(outstandingTasks.findIndex(t => t.id === "e"), 1);

        // Round-trip through JSON
        assertEquals(await promiseC, "c");
        tm.stop();
        tm = TaskManager.deserialize<TaskBase, string>(tm.serialize(), { onRunTask });
        tm.start();

        assertEquals(outstandingTasks.length, 2);
        assertEquals(outstandingTasks.findIndex(t => t.id === "d"), 0);
        assertEquals(outstandingTasks.findIndex(t => t.id === "e"), 1);

        // Advance time
        tm.stop();
        now = later(now, 1000 + 1);
        tm.start();

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

        const p = tm.run({ id: "c" }); // One to fill up the rate limit
        tm.run({ id: "a" });
        tm.run({ id: "a" }); // Duplicate; should be coalesced
        tm.run({ id: "b" });
        assertEquals(runCount, 1);

        // Need to let task completion handlers run
        await p;

        // Override the rate limit
        tm.stop();
        tm = TaskManager.deserialize<TaskBase, void>(tm.serialize(), {
            rateLimit: { count: 5, periodMS: 1000 },
            onRunTask,
        });

        now = later(now, 5000);
        tm.start();
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

Deno.test({
    name: "TaskManager scheduling",
    fn: async () => {
        const outstandingTasks: TaskBase[] = [];
        const rateLimit = { count: 3, periodMS: 1000 };
        const onRunTask = (t: TaskBase) => {
            outstandingTasks.splice(outstandingTasks.indexOf(t), 1);
            return Promise.resolve(t.id);
        };

        let tm = new TaskManager<TaskBase, string>({ rateLimit, onRunTask });

        function runTask(s: string, d?: Date) {
            const t = { id: s};
            outstandingTasks.push(t);
            return tm.run(t, d);
        }

        runTask("a");
        assertEquals(runTask("b", later(now, 2000)), null);
        runTask("c");
        const promiseD = runTask("d");
        runTask("e");

        // a, c, d should have run
        assertEquals(outstandingTasks.length, 2);
        assertEquals(outstandingTasks.findIndex(t => t.id === "b"), 0);
        assertEquals(outstandingTasks.findIndex(t => t.id === "e"), 1);

        // Round-trip through JSON
        assertEquals(await promiseD, "d");
        tm.stop();
        tm = TaskManager.deserialize<TaskBase, string>(tm.serialize(), { onRunTask });
        tm.start();

        assertEquals(outstandingTasks.length, 2);
        assertEquals(outstandingTasks.findIndex(t => t.id === "b"), 0);
        assertEquals(outstandingTasks.findIndex(t => t.id === "e"), 1);

        // Advance time
        tm.stop();
        now = later(now, 1000 + 1);
        tm.start();

        // e should have run
        assertEquals(outstandingTasks.length, 1);
        assertEquals(outstandingTasks.findIndex(t => t.id === "b"), 0);

        // Advance time again
        tm.stop();
        now = later(now, 1000 + 1);
        tm.start();

        // All should be done now
        assertEquals(outstandingTasks.length, 0);
    },
});
