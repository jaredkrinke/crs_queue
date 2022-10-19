/** Represents a rate limit as count per period. */
export interface RateLimit {
    /** Maximum count for the period. */
    count: number;

    /** Period (in milliseconds). */
    periodMS: number;
}

interface RateLimiterJsonSource {
    rate: RateLimit;
    history: Date[];
}

interface RateLimiterJsonResult {
    rate: RateLimit;
    history: string[];
}

// TODO: Replace with "satisfies" once that's supported...
function identity<T>(o: T): T { return o; }

/** Limits the number of requests to no more than the given rate. */
export class RateLimiter {
    private readonly history: Date[];
    private rate: RateLimit;

    /** Creates a rate limiter for the given rate, optionally providing a preexisting rate limiting history. */
    constructor(rateLimit: RateLimit, history?: Date[]) {
        this.rate = rateLimit;
        this.history = history ?? [];
    }

    /** Deserializes a RateLimiter from a JSON string. */
    public static deserialize(json: string): RateLimiter {
        const o = JSON.parse(json) as RateLimiterJsonResult;
        return new RateLimiter(o.rate, o.history?.map(s => new Date(s)));
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

    /** Attempts to initiate a request. Returns true if the request is allowed; otherwise returns the time in the future at which *any* request would be allowed.
     * 
     * Note: The reference time defaults to the current time, but this can be overridden. Take care to ensure that the times supplied are monotonically increasing.
     */
    public tryRequest(now = new Date()): boolean | Date {
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

    /** Serializes the RateLimiter to a JSON string. */
    public serialize(): string {
        return JSON.stringify(identity<RateLimiterJsonSource>({
            rate: this.rate,
            history: this.history,
        }));
    }
}

/** Base type for tasks. */
export type TaskBase = {
    /** Identity of the task. Tasks with the same `id` property will be coalesced. */
    id: string,
};

/** Options for creating TaskManager. */
export type TaskManagerOptions<TTask extends TaskBase, TResult> = {
    /** Rate limit for running tasks. */
    rateLimit: RateLimit;

    /** Callback for running the given task. */
    onRunTask: (task: TTask) => Promise<TResult>;

    /** Callback for handling failure of the given task (e.g. to schedule retries). */
    onTaskFailure?: (task: TTask) => void;
}

/** Options for deserializing a TaskManager. */
export type TaskManagerDeserializeOptions<TTask extends TaskBase, TResult> = {
    /** (Optional) New rate limit to apply (this overrides the serialized rate limit). This is useful if the rate limit
     * needs to be changed but the task manager was serialized with an old rate limit. */
    rateLimit?: RateLimit;

    /** Callback for running the given task. */
    onRunTask: (task: TTask) => Promise<TResult>;

    /** Callback for handling failure of the given task (e.g. to schedule retries). */
    onTaskFailure?: (task: TTask) => void;
}

/** State for a TaskManager (used for deserializing a TaskManager). */
export type TaskManagerState<TTask extends TaskBase> = {
    rateLimiter: RateLimiter;
    queue: TTask[];
}

type TaskManagerJson<TTask extends { id: string }> = {
    limiterJson: string;
    queue: TTask[];
};

/** A coalescing, rate-limited, serializable/deserializable task manager. */
export class TaskManager<TTask extends TaskBase, TResult> {
    // Persistent state
    private limiter: RateLimiter;
    private onRunTask: (task: TTask) => Promise<TResult>;
    private onTaskFailure?: (task: TTask) => void;
    private queue: TTask[];

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
        this.stopped = true;
        this.running = [];
    }

    private static insertOrReplaceTaskInto<TTask extends TaskBase>(queue: TTask[], task: TTask, replace: boolean): void {
        const existingIndex = queue.findIndex(t => t.id === task.id);
        if (existingIndex >= 0) {
            // Task with same id already exists in queue; replace or drop as requested
            if (replace) {
                queue.splice(existingIndex, 1, task);
            }
        } else {
            queue.push(task);
        }
    }

    /** Deserializes a TaskManager from a JSON string, using the given options. */
    public static deserialize<TTask extends TaskBase, TResult>(json: string, options: TaskManagerDeserializeOptions<TTask, TResult>): TaskManager<TTask, TResult> {
        const o = JSON.parse(json) as TaskManagerJson<TTask>;
        const rateLimiter = RateLimiter.deserialize(o.limiterJson);
        return new TaskManager<TTask, TResult>({
            rateLimit: options.rateLimit ?? rateLimiter.getRateLimit(),
            onRunTask: options.onRunTask,
            onTaskFailure: options.onTaskFailure,
        }, {
            rateLimiter,
            queue: o.queue,
        });
    }

    private drain(triggeringTask?: TTask, now = new Date()): Promise<TResult> | null {
        if (this.stopped) {
            throw new Error("Attempted to run TaskManager while it was stopped!");
        }

        if (this.callbackToken !== undefined) {
            // Timer is already scheduled
            return null;
        }

        let promiseOrNull: Promise<TResult> | null = null;
        while (this.queue.length > 0) {
            const result = this.limiter.tryRequest(now);
            if (result === true) {
                // Under the rate limit; run the task
                const task = this.queue.shift()!;
                this.running.push(task);

                const promise = this.onRunTask(task);
                promise.then(() => {
                    // Task completed
                    if (!this.stopped) {
                        this.running.splice(this.running.indexOf(task), 1);
                    }
                }).catch(() => {
                    // Task failed
                    if (!this.stopped) {
                        this.running.splice(this.running.indexOf(task), 1);
                        if (this.onTaskFailure) {
                            this.onTaskFailure(task);
                        }
                    }
                });

                // Check to see if this was the triggering task; if so, this promise should be returned
                if (task.id === triggeringTask?.id) {
                    promiseOrNull = promise;
                }
            } else {
                // Over the rate limit; schedule a timer
                const nextRunTime = result as Date;
                this.callbackToken = setTimeout(() => {
                    this.callbackToken = undefined;
                    this.drain();
                }, nextRunTime.valueOf() - now.valueOf());
                break;
            }
        }

        return promiseOrNull;
    }

    private insertOrReplaceTask(task: TTask, replace: boolean): void {
        TaskManager.insertOrReplaceTaskInto<TTask>(this.queue, task, replace);
    }

    /** Starts executing tasks. */
    public start(now = new Date()): void {
        this.stopped = false;
        this.drain(undefined, now);
    }

    /** Adds (and attempts to run, if possible) a task. */
    public run(task: TTask, now = new Date()): Promise<TResult> | null {
        this.insertOrReplaceTask(task, true);
        return this.drain(task, now);
    }

    /** Stops task processing. Note that any in-progress tasks cannot be canceled/stopped; this function only affects
     * queued tasks. */
    public stop(): void {
        this.stopped = true;
        if (this.callbackToken) {
            clearTimeout(this.callbackToken);
            this.callbackToken = undefined;
        }
    }

    /** Serializes a TaskManager and its queued/running tasks to a JSON string. */
    public serialize(): string {
        // Merge running and queued tasks for serialization with queued tasks taking precedence (since they're newer)
        const queue = this.queue.slice();
        for (const task of this.running) {
            TaskManager.insertOrReplaceTaskInto<TTask>(queue, task, false);
        }

        return JSON.stringify(identity<TaskManagerJson<TTask>>({
            limiterJson: this.limiter.serialize(),
            queue,
        }));
    }
}
