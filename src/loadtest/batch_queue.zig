const std = @import("std");

const PathEdit = @import("../pathEdit.zig");
const Edit = @import("../edit.zig");

pub const BatchQueue = @This();

queue: []?[]const Edit,
mutex: std.Thread.Mutex,
condition: std.Thread.Condition,
state: struct {
    head: u64,
    tail: u64,
    count: u64,
    done: bool,
},

pub fn init(capacity: u64, alloc: std.mem.Allocator) !*BatchQueue {
    const q = try alloc.create(BatchQueue);
    q.* = BatchQueue{
        .queue = try alloc.alloc(?[]const Edit, capacity),
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
pub fn push(self: *BatchQueue, edits: []const Edit) anyerror!void {
    self.queue[self.state.tail] = edits;
    self.state.tail = (self.state.tail + 1) % self.queue.len;
    self.state.count += 1;
}

/// read from head
pub fn pop(self: *BatchQueue) ?[]const Edit {
    const edits = self.queue[self.state.head];
    if (edits == null) {
        return null;
    }
    self.state.head = (self.state.head + 1) % self.queue.len;
    self.state.count -= 1;
    return edits;
}

pub fn close(self: *BatchQueue) void {
    _ = self;
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

test "multi threading" {}
