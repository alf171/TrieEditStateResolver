const std = @import("std");

const PathEdit = @import("../pathEdit.zig");
const Edit = @import("../edit.zig");

pub const BatchQueue = @This();
const ns_per_ms = 1_000_000;

queue: []?[]const Edit,
mutex: std.Thread.Mutex,
condition: std.Thread.Condition,
state: struct {
    head: usize,
    tail: usize,
    count: usize,
    done: bool,
},

pub fn init(capacity: usize, alloc: std.mem.Allocator) !*BatchQueue {
    const q = try alloc.create(BatchQueue);
    errdefer alloc.destroy(q);
    const queue = try alloc.alloc(?[]const Edit, capacity);
    @memset(queue, null);
    q.* = BatchQueue{
        .queue = queue,
        .mutex = .{},
        .condition = .{},
        .state = .{ .head = 0, .tail = 0, .count = 0, .done = false },
    };
    return q;
}

pub fn deinit(self: *BatchQueue, alloc: std.mem.Allocator) void {
    alloc.free(self.queue);
    alloc.destroy(self);
}

/// write at tail
pub fn push(self: *BatchQueue, batch: []const Edit) error{Closed}!void {
    self.mutex.lock();
    defer self.mutex.unlock();

    while (self.state.count == self.queue.len and !self.state.done) {
        self.condition.wait(&self.mutex);
    }

    if (self.state.done) {
        return error.Closed;
    }

    self.queue[self.state.tail] = batch;
    self.state.tail = (self.state.tail + 1) % self.queue.len;
    self.state.count += 1;
    self.condition.signal();
}

/// read from head
pub fn pop(self: *BatchQueue) ?[]const Edit {
    self.mutex.lock();
    defer self.mutex.unlock();

    while (self.state.count == 0 and !self.state.done) {
        self.condition.wait(&self.mutex);
    }

    if (self.state.count == 0 and self.state.done) {
        return null;
    }

    const edits = self.queue[self.state.head];
    self.queue[self.state.head] = null;
    if (edits == null) {
        return null;
    }
    self.state.head = (self.state.head + 1) % self.queue.len;
    self.state.count -= 1;
    self.condition.signal();
    return edits;
}

pub fn close(self: *BatchQueue) void {
    self.mutex.lock();
    defer self.mutex.unlock();
    self.state.done = true;
    self.condition.broadcast();
}

test "create queue" {
    const alloc = std.testing.allocator;
    const q = try BatchQueue.init(16, alloc);
    q.deinit(alloc);
}

test "single thread" {
    const alloc = std.testing.allocator;
    const q = try BatchQueue.init(16, alloc);
    defer q.deinit(alloc);

    const p1 = [_]PathEdit{.{ .path = "a", .value = "foobar" }};
    const p2 = [_]PathEdit{.{ .path = "b", .value = "foobar" }};
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
    const q = try BatchQueue.init(16, alloc);
    defer q.deinit(alloc);

    const producer = struct {
        fn run(self: *BatchQueue, batch: []const Edit) !void {
            try self.push(batch);
        }
    };
    const consumer = struct {
        fn run(self: *BatchQueue, output: *?[]const Edit) void {
            output.* = self.pop();
        }
    };

    const path_edits = [_]PathEdit{.{ .path = "a", .value = "foobar" }};
    const batch = [_]Edit{.{ .pathEdits = path_edits[0..], .timestamp = 1 }};
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
    const q = try BatchQueue.init(16, alloc);
    defer q.deinit(alloc);
    var started = std.atomic.Value(bool).init(false);
    var finished = std.atomic.Value(bool).init(false);

    const producer = struct {
        fn run(self: *BatchQueue, batch: []const Edit) !void {
            try self.push(batch);
        }
    };
    const consumer = struct {
        fn run(self: *BatchQueue, output: *?[]const Edit, started_flag: *std.atomic.Value(bool), finished_flag: *std.atomic.Value(bool)) void {
            started_flag.store(true, .release);
            output.* = self.pop();
            finished_flag.store(true, .release);
        }
    };

    const path_edits = [_]PathEdit{.{ .path = "a", .value = "foobar" }};
    const batch = [_]Edit{.{ .pathEdits = path_edits[0..], .timestamp = 1 }};
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
