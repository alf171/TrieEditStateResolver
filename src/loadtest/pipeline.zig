const std = @import("std");

const ApplyEdits = @import("../apply_edits.zig");
const Config = @import("config.zig").Config;
const ConcurrentQueue = @import("../queues/concurrent_queue.zig");
const Database = @import("../database.zig");
const Document = @import("../document.zig");
const LockingDocument = @import("../locking_document.zig");
const Edit = @import("../edit.zig");
const Generator = @import("../generator.zig");
const Set = @import("../set.zig");

const Metrics = Config.Metrics;
const Timer = std.time.Timer;
const ns_per_ms = 1_000_000;

pub const PipelineScenario = @This();

document: Document,
database: *Database,
queue: *ConcurrentQueue,

pub fn initFn(ptr: *anyopaque, config: Config, alloc: std.mem.Allocator) anyerror!void {
    const self: *PipelineScenario = @ptrCast(@alignCast(ptr));
    const paths = try Set.of(&.{ "a/b", "a/c", "a/d", "a/e", "1/2", "1/3", "1/4" }, alloc);
    const editGenerator = try alloc.create(Generator.EditGenerator);
    editGenerator.* = Generator.EditGenerator{
        .batch_size = config.database_batch_size,
        .min_value_len = 10,
        .max_value_len = 20,
        .path_edits_per_edit = 1,
        .prng = std.Random.DefaultPrng.init(config.seed),
        .set_of_paths = paths.data,
        .timestamp_order = .RANDOM,
        .actions = .PUT_AND_DELETE,
    };

    self.database = try Database.init(editGenerator, config.database_latency_ms, alloc);
    const timestamp = std.time.milliTimestamp();
    const DocType = @TypeOf(self.document);
    self.document = try DocType.init(timestamp, alloc);
    self.queue = try ConcurrentQueue.init(16, alloc);
}

pub fn runFn(ptr: *anyopaque, config: Config, alloc: std.mem.Allocator) anyerror!Metrics {
    const self: *PipelineScenario = @ptrCast(@alignCast(ptr));

    const producer = struct {
        fn run(pqueue: *ConcurrentQueue, database: *Database, total_batches: usize, fetch_ns: *std.atomic.Value(u64), palloc: std.mem.Allocator) !void {
            defer pqueue.close();
            var t = try Timer.start();
            while (database.cursor < total_batches) {
                try pqueue.push(try database.nextPage(palloc));
            }
            _ = fetch_ns.fetchAdd(t.read(), .acquire);
        }
    };
    const consumer = struct {
        fn run(pqueue: *ConcurrentQueue, document: *@TypeOf(self.document), apply_ns: *std.atomic.Value(u64), calloc: std.mem.Allocator) !void {
            while (pqueue.pop()) |edits| {
                var t = try Timer.start();
                try ApplyEdits.apply(document, edits, calloc, .SEQUENTIAL);
                _ = apply_ns.fetchAdd(t.read(), .monotonic);
            }
        }
    };
    var total_timer = try Timer.start();
    var fetch_ns = std.atomic.Value(u64).init(0);
    var apply_ns = std.atomic.Value(u64).init(0);

    const thread_producer = try std.Thread.spawn(.{ .allocator = alloc }, producer.run, .{ self.queue, self.database, config.total_batches, &fetch_ns, alloc });
    const thread_consumer = try std.Thread.spawn(.{ .allocator = alloc }, consumer.run, .{ self.queue, &self.document, &apply_ns, alloc });

    thread_producer.join();
    thread_consumer.join();
    const total_time_ms = total_timer.read() / ns_per_ms;
    const fetch_latency_ms = fetch_ns.load(.acquire) / ns_per_ms;
    const apply_latency_ms = apply_ns.load(.acquire) / ns_per_ms;

    return .{ .fetch_latency_ms = fetch_latency_ms, .apply_latency_ms = apply_latency_ms, .total_time_ms = total_time_ms };
}

pub fn deinitFn(ptr: *anyopaque, alloc: std.mem.Allocator) void {
    const self: *PipelineScenario = @ptrCast(@alignCast(ptr));
    self.database.deinit(alloc);
    self.database.generator.deinit(alloc);
    self.document.free(alloc);
    self.queue.deinit(alloc);
}
