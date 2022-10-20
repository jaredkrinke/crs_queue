/** Represents a rate limit as count per period. */
export interface RateLimit {
    /** Maximum count for the period. */
    count: number;

    /** Period (in milliseconds). */
    periodMS: number;
}

interface RateLimiterJson {
    rate: RateLimit;
    history: string[];
}

/** Limits the number of requests to no more than the given rate. */
export class RateLimiter {
    private static getNow = () => new Date();

    private readonly history: Date[];
    private rate: RateLimit;

    /** Creates a rate limiter for the given rate, optionally providing a preexisting rate limiting history. */
    constructor(rateLimit: RateLimit, history?: Date[]) {
        this.rate = rateLimit;
        this.history = history ?? [];
    }

    /** Reconstructs a RateLimiter from parsed JSON. */
    public static fromParsedJson(data: RateLimiterJson): RateLimiter {
        return new RateLimiter(data.rate, data.history.map(s => new Date(s)));
    }

    /** Deserializes a RateLimiter from a JSON string. */
    public static deserialize(json: string): RateLimiter {
        return RateLimiter.fromParsedJson(JSON.parse(json) as RateLimiterJson);
    }

    private addPeriod(date: Date): Date {
        return new Date(date.valueOf() + this.rate.periodMS);
    }

    private update(now: Date): void {
        while (this.history.length > 0 && (this.addPeriod(this.history[0]) < now)) {
            this.history.shift();
        }
    }

    private addRequest(now: Date): void {
        this.history.push(now);
    }

    /** Attempts to initiate a request. Returns true if the request is allowed; otherwise returns the time in the
     * future at which *any* request would be allowed.
     */
    public tryRequest(): boolean | Date {
        const now = RateLimiter.getNow();
        this.update(now);
        if (this.history.length < this.rate.count) {
            this.addRequest(now);
            return true;
        } else {
            return this.addPeriod(this.history[0]);
        }
    }

    /** Get the rate limit. This is used internally, but is probably not needed for consumers of this library. */
    public getRateLimit(): RateLimit {
        return this.rate;
    }

    /** Set the rate limit. This is used internally on deserialize in case the rate needs to be adjusted. It is not recommended that consumers of this library call this function. */
    public setRateLimit(rate: RateLimit): void {
        this.rate = rate;
    }

    /** Saves the RateLimiter to a JSON-friendly object. */
    public toParsedJson(): RateLimiterJson {
        this.update(RateLimiter.getNow());
        const json: RateLimiterJson = {
            rate: this.rate,
            history: this.history.map(d => d.toJSON()),
        };

        return json;
    }

    /** Serializes the RateLimiter to a JSON string. */
    public serialize(): string {
        return JSON.stringify(this.toParsedJson());
    }
}

/** Base type for tasks. */
export type TaskBase = {
    /** Identity of the task. Tasks with the same `id` property will be coalesced. */
    id: string,
};

type TaskEntry<TTask extends TaskBase> = {
    task: TTask;
    start: Date;
};

type TaskEntryJson<TTask extends TaskBase> = {
    task: TTask;
    start: string;
};

/** Options for creating TaskManager. */
export type TaskManagerOptions<TTask extends TaskBase, TResult> = {
    /** Rate limit for running tasks. */
    rateLimit: RateLimit;

    /** Callback for running the given task. */
    onRunTask: (task: TTask) => Promise<TResult>;

    /** Callback for handling failure of the given task (e.g. to schedule retries). */
    onTaskFailure?: (task: TTask, reason?: Error | string) => void;
}

/** Options for deserializing a TaskManager. */
export type TaskManagerDeserializeOptions<TTask extends TaskBase, TResult> = {
    /** (Optional) New rate limit to apply (this overrides the serialized rate limit). This is useful if the rate limit
     * needs to be changed but the task manager was serialized with an old rate limit. */
    rateLimit?: RateLimit;

    /** Callback for running the given task. */
    onRunTask: (task: TTask) => Promise<TResult>;

    /** Callback for handling failure of the given task (e.g. to schedule retries). */
    onTaskFailure?: (task: TTask, reason?: Error | string) => void;
}

/** State for a TaskManager (used for deserializing a TaskManager). */
export type TaskManagerState<TTask extends TaskBase> = {
    rateLimiter: RateLimiter;
    queue: TaskEntry<TTask>[];
}

type TaskManagerJson<TTask extends { id: string }> = {
    limiter: RateLimiterJson;
    queue: TaskEntryJson<TTask>[];
};

/** A coalescing, rate-limited, serializable/deserializable task manager. */
export class TaskManager<TTask extends TaskBase, TResult> {
    private static getNow = () => new Date();

    // Persistent state
    private readonly limiter: RateLimiter;
    private readonly onRunTask: (task: TTask) => Promise<TResult>;
    private readonly onTaskFailure?: (task: TTask, reason?: Error | string) => void;
    private readonly queue: TaskEntry<TTask>[];

    // Transient state
    private stopped: boolean;
    private running: TTask[];
    private callbackToken?: number;

    /** Creates a new TaskManager with the given options (and optional state). */
    constructor(options: TaskManagerOptions<TTask, TResult>, state?: TaskManagerState<TTask>) {
        if (state?.rateLimiter) {
            this.limiter = state.rateLimiter;

            // Update the rate limit, in case it is different from what was serialized
            this.limiter.setRateLimit(options.rateLimit);
        } else {
            this.limiter = new RateLimiter(options.rateLimit);
        }

        this.onRunTask = options.onRunTask;
        this.onTaskFailure = options.onTaskFailure;
        this.queue = (state?.queue) ?? [];
        this.stopped = false;
        this.running = [];

        this.drain(TaskManager.getNow());
    }

    private static insertOrReplaceTaskInto<TTask extends TaskBase>(queue: TaskEntry<TTask>[], task: TTask, replace: boolean, earliestStartTime?: Date): void {
        // Remove any existing task with the same id, if needed
        const existingIndex = queue.findIndex(t => t.task.id === task.id);
        if (existingIndex >= 0) {
            if (replace) {
                queue.splice(existingIndex, 1);
            } else {
                // Not replacing and already exists; all done
                return;
            }
        }

        const start = earliestStartTime ?? TaskManager.getNow();
        const taskEntry: TaskEntry<TTask> = { task, start };

        // Insert, sorted by start time
        let i = 0;
        while (i < queue.length && queue[i].start <= start) {
            ++i;
        }
        queue.splice(i, 0, taskEntry);
    }

    /** Reconstructs a TaskManager from parsed JSON, using the given options. */
    public static fromParsedJson<TTask extends TaskBase, TResult>(data: TaskManagerJson<TTask>, options: TaskManagerDeserializeOptions<TTask, TResult>): TaskManager<TTask, TResult> {
        const rateLimiter = RateLimiter.fromParsedJson(data.limiter);
        return new TaskManager<TTask, TResult>({
            rateLimit: options.rateLimit ?? rateLimiter.getRateLimit(),
            onRunTask: options.onRunTask,
            onTaskFailure: options.onTaskFailure,
        }, {
            rateLimiter,
            queue: data.queue.map(e => ({ ...e, start: new Date(e.start) })),
        });
    }

    /** Deserializes a TaskManager from a JSON string, using the given options. */
    public static deserialize<TTask extends TaskBase, TResult>(json: string, options: TaskManagerDeserializeOptions<TTask, TResult>): TaskManager<TTask, TResult> {
        return TaskManager.fromParsedJson<TTask, TResult>(JSON.parse(json) as TaskManagerJson<TTask>, options);
    }

    private unscheduleCallback(): void {
        if (this.callbackToken) {
            clearTimeout(this.callbackToken);
            this.callbackToken = undefined;
        }
    }

    private drain(now: Date, triggeringTask?: TTask): Promise<TResult> | null {
        if (this.stopped) {
            return null;
        }

        let promiseOrNull: Promise<TResult> | null = null;
        let nextRunTime: Date | undefined;
        while (this.queue.length > 0) {
            // Peek at the next task's start time
            const { start } = this.queue[0];
            if (start <= now) {
                // Task can be run; see if allowed by the rate limit
                const result = this.limiter.tryRequest();
                if (result === true) {
                    // Under the rate limit; run it
                    const { task } = this.queue.shift()!;
                    this.running.push(task);
    
                    const promise = this.onRunTask(task);
                    promise.then(() => {
                        // Task completed
                        this.running.splice(this.running.indexOf(task), 1);
                    }).catch((reason) => {
                        // Task failed
                        this.running.splice(this.running.indexOf(task), 1);
                        if (this.onTaskFailure) {
                            this.onTaskFailure(task, (reason instanceof Error || typeof(reason) === "string") ? reason : undefined);
                        }
                    });
    
                    // Check to see if this was the triggering task; if so, this promise should be returned
                    if (task.id === triggeringTask?.id) {
                        promiseOrNull = promise;
                    }
                } else {
                    // Over the rate limit; schedule a timer
                    nextRunTime = result as Date;
                }
            } else {
                // It's not yet time for this task; schedule a timer
                nextRunTime = start;
            }

            if (nextRunTime !== undefined) {
                // Need to schedule a timer for the next task; do so and then break out of the loop
                this.unscheduleCallback();
                this.callbackToken = setTimeout(() => {
                    this.callbackToken = undefined;
                    this.drain(TaskManager.getNow());
                }, nextRunTime.valueOf() - now.valueOf());

                break;
            }
        }

        return promiseOrNull;
    }

    /** Starts executing tasks, if previously stopped. */
    public start(): void {
        const now = TaskManager.getNow();
        this.stopped = false;
        this.drain(now);
    }

    /** Adds (and attempts to run, if possible) a task. Tasks can be scheduled for the future by providing earliestStartTime. */
    public run(task: TTask, earliestStartTime?: Date): Promise<TResult> | null {
        const now = TaskManager.getNow();
        TaskManager.insertOrReplaceTaskInto<TTask>(this.queue, task, true, earliestStartTime);
        return this.drain(now, task);
    }

    /** Stops task processing. Note that any in-progress tasks cannot be canceled/stopped; this function only affects
     * queued tasks. */
    public stop(): void {
        this.stopped = true;
        this.unscheduleCallback();
    }

    /** Saves the TaskManager and its queued/running tasks to a JSON-friendly object. */
    public toParsedJson(): TaskManagerJson<TTask> {
        // Merge running and queued tasks for serialization with queued tasks taking precedence (since they're newer)
        const queue = this.queue.slice();
        for (const task of this.running) {
            TaskManager.insertOrReplaceTaskInto<TTask>(queue, task, false);
        }

        const json: TaskManagerJson<TTask> = {
            limiter: this.limiter.toParsedJson(),
            queue: queue.map(te => ({ task: te.task, start: te.start.toJSON() })),
        }

        return json;
    }

    /** Serializes the TaskManager and its queued/running tasks to a JSON string. */
    public serialize(): string {
        return JSON.stringify(this.toParsedJson());
    }
}
