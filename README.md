# crs_queue
`crs_queue` is a simple coalescing, rate-limited, serializable/deserializable task queue for Node and Deno which is designed for issuing requests to a rate-limited endpoint from a host program that is transient (as opposed to long-running).

## Features

* Tasks with the same (string) identity are coalesced (using "last writer wins" for task data)
* Requests are limited according to a user-specified rate
* The entire queue (including tasks, data, history) can be serialized/deserialized (e.g. to localStorage)

Note that this library does not *guarantee* that requests won't be issued more than once. For example, if the queue is serialized while a request is in flight, that request can later be run a second time if the queue is deserialized and restarted.

## API
### Task type
Tasks are objects with only primitive/JSON-compatible properties. There is only one required property:

* `id` (string): This is the identity of the task; tasks with the same identity are coalesced (with all other data being overwritten by the most recent task)

Example:

```js
{
    id: "update leaderboard",
    value: 5,
}
```

### TaskManager
This section is just a quick summary. Refer to JSDoc comments for more detail.

The task queue is encapsulated in the `TaskManager<TTask extends TaskBase, TResult>` class. The class constructor takes `options` object with the following properties:

* `rateLimit` (`{ count: number, periodMS: number}`):
  * count: Maximum number of tasks to process during the period
  * periodMS: Length of the period in milliseconds
* `onRunTask` (`(task: TTask) => Promise<TResult>`): Async callback for processing a task
* `onTaskFailure` (`(task: TTask) => void | undefined`): Optional task failure handler (e.g. to re-enqueue the task)

Here's a brief description of the class's methods:

* `start(): void`: Starts task processing
* `run(task: TTask): Promise<TResult> | null`: Enqueues the provided task (coalescing, if applicable); if run immediately, the running task's promise (from `onRunTask` above) is returned (otherwise null is returned)
* `stop(): void`: Halts task processing (note that in-flight tasks *will not* be canceled/stopped)
* `serialize(): string`: Serializes the task queue to string

Deserialization is done using:

* `TaskManager.deserialize(json: string, options)`
