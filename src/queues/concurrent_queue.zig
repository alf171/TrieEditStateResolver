const std = @import("std");

const PathEdit = @import("../pathEdit.zig").PathEdit;
const EditAction = @import("../pathEdit.zig").EditAction;
const pathEditPut = @import("../pathEdit.zig").initPut;
const Edit = @import("../edit.zig");
const RingBuffer = @import("ring_buffer.zig").RingBuffer;

pub const ConcurrentQueue = @This();
const ns_per_ms = 1_000_000;

queue: *RingBuffer([]Edit),
mutex: std.Thread.Mutex,
condition: std.Thread.Condition,
done: bool,

pub fn init(capacity: usize, alloc: std.mem.Allocator) !*ConcurrentQueue {
    const q = try alloc.create(ConcurrentQueue);
    errdefer alloc.destroy(q);
    q.* = ConcurrentQueue{
        .queue = try RingBuffer([]Edit).init(capacity, alloc),
        .mutex = .{},
        .condition = .{},
        .done = false,
    };
    return q;
}

pub fn deinit(self: *ConcurrentQueue, alloc: std.mem.Allocator) void {
    self.queue.deinit(alloc);
    alloc.destroy(self);
}

/// write at tail
pub fn push(self: *ConcurrentQueue, batch: []Edit) error{ Full, Closed }!void {
    self.mutex.lock();
    defer self.mutex.unlock();

    while (self.queue.isFull() and !self.done) {
        self.condition.wait(&self.mutex);
    }

    if (self.done) return error.Closed;

    try self.queue.push(batch);
    self.condition.signal();
}

/// read from head
pub fn pop(self: *ConcurrentQueue) ?[]Edit {
    self.mutex.lock();
    defer self.mutex.unlock();

    while (self.queue.isEmpty() and !self.done) {
        self.condition.wait(&self.mutex);
    }

    if (self.done and self.queue.isEmpty()) {
        return null;
    }

    const edits = self.queue.pop();
    self.condition.signal();
    return edits;
}

pub fn close(self: *ConcurrentQueue) void {
    self.mutex.lock();
    defer self.mutex.unlock();
    self.done = true;
    self.condition.broadcast();
}

test "create queue" {
    const alloc = std.testing.allocator;
    const q = try ConcurrentQueue.init(16, alloc);
    q.deinit(alloc);
}

test "single thread" {
    const alloc = std.testing.allocator;
    const q = try ConcurrentQueue.init(16, alloc);
    defer q.deinit(alloc);

    const p1 = [_]PathEdit{pathEditPut("a", .{ .string = "foobar" })};
    const p2 = [_]PathEdit{pathEditPut("b", .{ .string = "foobar" })};
    const b1 = [_]Edit{.{
        .pathEdits = p1[0..],
        .timestamp = 1,
    }};
    const b2 = [_]Edit{.{
        .pathEdits = p2[0..],
        .timestamp = 2,
    }};

    try q.push(b1[0..]);
    try q.push(b2[0..]);
    const e1 = q.pop() orelse {
        return error.EmptyQueue;
    };
    const e2 = q.pop() orelse {
        return error.EmptyQueue;
    };

    // fifo ordering
    try std.testing.expectEqual(@as(usize, 1), e1.len);
    try std.testing.expectEqual(@as(i64, 1), e1[0].timestamp);
    try std.testing.expectEqual(@as(usize, 1), e2.len);
    try std.testing.expectEqual(@as(i64, 2), e2[0].timestamp);
}

test "multi threading" {
    const alloc = std.testing.allocator;
    const q = try ConcurrentQueue.init(16, alloc);
    defer q.deinit(alloc);

    const producer = struct {
        fn run(self: *ConcurrentQueue, batch: []const Edit) !void {
            try self.push(batch);
        }
    };
    const consumer = struct {
        fn run(self: *ConcurrentQueue, output: *?[]const Edit) void {
            output.* = self.pop();
        }
    };

    const pathEdits = [_]PathEdit{pathEditPut("a", .{ .string = "foobar" })};
    const batch = [_]Edit{.{ .pathEdits = pathEdits[0..], .timestamp = 1 }};
    const thread_producer = try std.Thread.spawn(.{ .allocator = alloc }, producer.run, .{ q, batch[0..] });
    var output: ?[]const Edit = null;
    const thread_consumer = try std.Thread.spawn(.{ .allocator = alloc }, consumer.run, .{ q, &output });
    thread_producer.join();
    thread_consumer.join();

    try std.testing.expect(output != null);
    try std.testing.expectEqual(@as(usize, 1), output.?.len);
    try std.testing.expectEqual(@as(i64, 1), output.?[0].timestamp);
}

test "wake up" {
    const alloc = std.testing.allocator;
    const q = try ConcurrentQueue.init(16, alloc);
    defer q.deinit(alloc);
    var started = std.atomic.Value(bool).init(false);
    var finished = std.atomic.Value(bool).init(false);

    const producer = struct {
        fn run(self: *ConcurrentQueue, batch: []const Edit) !void {
            try self.push(batch);
        }
    };
    const consumer = struct {
        fn run(self: *ConcurrentQueue, output: *?[]const Edit, started_flag: *std.atomic.Value(bool), finished_flag: *std.atomic.Value(bool)) void {
            started_flag.store(true, .release);
            output.* = self.pop();
            finished_flag.store(true, .release);
        }
    };

    const pathEdits = [_]PathEdit{pathEditPut("a", .{ .string = "foobar" })};
    const batch = [_]Edit{.{ .pathEdits = pathEdits[0..], .timestamp = 1 }};
    var output: ?[]const Edit = null;
    const thread_consumer = try std.Thread.spawn(.{ .allocator = alloc }, consumer.run, .{ q, &output, &started, &finished });
    std.Thread.sleep(10 * ns_per_ms);
    try std.testing.expect(started.load(.acquire));
    try std.testing.expect(!finished.load(.acquire));

    const thread_producer = try std.Thread.spawn(.{ .allocator = alloc }, producer.run, .{ q, batch[0..] });

    thread_producer.join();
    thread_consumer.join();

    try std.testing.expect(output != null);
    try std.testing.expectEqual(@as(usize, 1), output.?.len);
    try std.testing.expectEqual(@as(i64, 1), output.?[0].timestamp);
    try std.testing.expect(finished.load(.acquire));
}
