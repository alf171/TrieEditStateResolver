const std = @import("std");
const NonBlockingQueue = @import("queues/non_blocking_queue.zig").NonBlockingQueue;
const Queue = @import("queues/ring_buffer.zig").RingBuffer;
const Edit = @import("edit.zig");
const Generator = @import("generator.zig");

// an simulated inmemory database
pub const Database = @This();

pub const PendingReq = struct {
    token: u64,
    ready_at_ns: i128,
};

pub const Completion = struct {
    token: u64,
    result: anyerror![]Edit,
};

database_latency_ms: u64,
generator: *Generator,
/// tells us where we are in batches
cursor: u64,

/// used for async only
ready_tasks: *NonBlockingQueue(PendingReq),
pending: *NonBlockingQueue(PendingReq),
completed: *NonBlockingQueue(Completion),

pub fn init(generator: *Generator, database_latency_ms: u64, alloc: std.mem.Allocator) !*Database {
    const database = try alloc.create(Database);
    database.* =
        .{
            .generator = generator,
            .database_latency_ms = database_latency_ms,
            .cursor = 0,
            .ready_tasks = try NonBlockingQueue(PendingReq).init(16, alloc),
            .pending = try NonBlockingQueue(PendingReq).init(16, alloc),
            .completed = try NonBlockingQueue(Completion).init(16, alloc),
        };
    return database;
}

pub fn deinit(self: *Database, alloc: std.mem.Allocator) void {
    self.ready_tasks.deinit(alloc);
    self.pending.deinit(alloc);
    self.completed.deinit(alloc);
    alloc.destroy(self);
}

/// return next page of edits. caller must free them
pub fn nextPage(self: *Database, alloc: std.mem.Allocator) ![]Edit {
    std.Thread.sleep(1000000 * sampleLatencyMs(self.generator.prng.random(), self.database_latency_ms));
    const edits = try self.generator.next_batch(alloc);
    self.cursor += 1;
    return edits;
}

pub fn asyncNextPage(self: *Database, token: u64) !void {
    const now_ns = std.time.nanoTimestamp();
    const offset_ns = 1000000 * sampleLatencyMs(self.generator.prng.random(), self.database_latency_ms);
    try self.pending.push(PendingReq{
        .ready_at_ns = now_ns + offset_ns,
        .token = token,
    });
}

pub fn tick(self: *Database, alloc: std.mem.Allocator) !void {
    // TODO: switch to some sort of priority based queue to avoid O(n) complexity

    var i: usize = 0;
    // move items from pending to ready
    while (i < self.pending.rb.count) : (i += 1) {
        const item = self.pending.pop() orelse break;
        if (item.ready_at_ns <= std.time.nanoTimestamp()) {
            try self.ready_tasks.push(item);
        } else {
            try self.pending.push(item);
        }
    }

    // move a single item from ready to completed
    if (self.ready_tasks.pop()) |item| {
        const result = self.generator.next_batch(alloc);
        self.cursor += 1;
        try self.completed.push(Completion{
            .token = item.token,
            .result = result,
        });
    }
}

pub fn pollForCompletion(self: *Database) error{NotCompleted}!Completion {
    return self.completed.pop() orelse error.NotCompleted;
}

fn sampleLatencyMs(rng: std.Random, base_ms: u64) u64 {
    const p = rng.float(f64);
    const b = @as(f64, @floatFromInt(@max(base_ms, 1)));

    var ms_f: f64 = 0.0;
    // ~0.8x..1.2x
    if (p < 0.85) {
        const u = rng.float(f64);
        ms_f = 0.8 * b + 0.4 * u * b;
    }
    // ~1.5x..2.5x
    else if (p < 0.99) {
        const u = rng.float(f64);
        ms_f = 1.5 * b + u * b;
    }
    // 5x..8x
    else {
        const u = rng.float(f64);
        ms_f = 5 * b + 3 * u * b;
    }
    return @intFromFloat(ms_f);
}

test "create db" {
    const alloc = std.testing.allocator;
    const generator = try Generator.default_init(0, alloc);
    defer generator.deinit(alloc);
    const db = try Database.init(generator, 10, alloc);
    defer db.deinit(alloc);
}

test "ready -> completed" {
    const alloc = std.testing.allocator;
    const generator = try Generator.default_init(0, alloc);
    defer generator.deinit(alloc);
    const db = try Database.init(generator, 0, alloc);
    defer db.deinit(alloc);

    try db.asyncNextPage(0);
    try std.testing.expect(db.completed.isEmpty());
    try db.tick(alloc);
    try std.testing.expect(!db.completed.isEmpty());
    const completion = try db.pollForCompletion();

    try std.testing.expectEqual(0, completion.token);
    const edits = try completion.result;
    for (try completion.result) |edit| {
        edit.free(alloc);
    }
    alloc.free(edits);
}
