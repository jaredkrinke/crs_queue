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


export type TaskBase = { id: string };

export type TaskManagerOptions<TTask extends TaskBase> = {
    rateLimit: RateLimit;
    onRunTask: (task: TTask) => Promise<void>;
    onTaskFailure?: (task: TTask) => void;
}

export type TaskManagerState<TTask extends TaskBase> = {
    rateLimiter: RateLimiter;
    queue: TTask[];
}

type TaskManagerJson<TTask extends { id: string }> = {
    limiterJson: string;
    queue: TTask[];
};

/** A rate-limited, coalescing, serializable/deserializable task manager. */
export class TaskManager<TTask extends TaskBase> {
    // Persistent state
    private limiter: RateLimiter;
    private onRunTask: (task: TTask) => Promise<void>;
    private onTaskFailure?: (task: TTask) => void;
    private queue: TTask[];

    // Transient state
    private stopped: boolean;
    private running: TTask[];
    private callbackToken?: number;

    constructor(options: TaskManagerOptions<TTask>, state?: TaskManagerState<TTask>) {
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

    public static deserialize<TTask extends TaskBase>(json: string, onRunTask: (task: TTask) => Promise<void>, onTaskFailure?: (task: TTask) => void, newRateLimit?: RateLimit): TaskManager<TTask> {
        const o = JSON.parse(json) as TaskManagerJson<TTask>;
        const rateLimiter = RateLimiter.deserialize(o.limiterJson);
        return new TaskManager<TTask>({
            rateLimit: newRateLimit ?? rateLimiter.getRateLimit(),
            onRunTask,
            onTaskFailure,
        }, {
            rateLimiter,
            queue: o.queue,
        });
    }

    private drain(now = new Date()): void {
        if (this.stopped) {
            throw new Error("Attempted to run TaskManager while it was stopped!");
        }

        if (this.callbackToken !== undefined) {
            // Timer is already scheduled
            return;
        }

        while (this.queue.length > 0) {
            const result = this.limiter.tryRequest(now);
            if (result === true) {
                // Under the rate limit; run the task
                const task = this.queue.shift()!;
                this.running.push(task);

                this.onRunTask(task).then(() => {
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
    }

    private insertOrReplaceTask(task: TTask, replace: boolean) {
        const existingIndex = this.queue.findIndex(t => t.id === task.id);
        if (existingIndex >= 0) {
            // Task with same id already exists in queue; replace or drop as requested
            if (replace) {
                this.queue.splice(existingIndex, 1, task);
            }
        }
    }

    public start(now = new Date()): void {
        this.stopped = false;
        this.drain(now);
    }

    public run(task: TTask, now = new Date()): void {
        this.insertOrReplaceTask(task, true);
        this.drain(now);
    }

    public stop(): void {
        this.stopped = true;
        if (this.callbackToken) {
            clearTimeout(this.callbackToken);
            this.callbackToken = undefined;
        }
    }

    public serialize(): string {
        this.stop();

        // Add running tasks back to the queue (dropping any that are already in the queue, since the ones in the queue are newer)
        for (const task of this.running) {
            this.insertOrReplaceTask(task, false);
        }

        return JSON.stringify(identity<TaskManagerJson<TTask>>({
            limiterJson: this.limiter.serialize(),
            queue: this.queue,
        }));
    }
}
