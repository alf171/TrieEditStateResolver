const std = @import("std");

const ringBuffer = @import("ring_buffer.zig");

pub fn NonBlockingQueue(comptime T: type) type {
    return struct {
        const Self = @This();
        rb: *ringBuffer.RingBuffer(T),
        closed: bool,

        pub fn init(cap: usize, alloc: std.mem.Allocator) !*Self {
            const q = try alloc.create(Self);
            const rb = try ringBuffer.RingBuffer(T).init(cap, alloc);
            q.* = Self{
                .rb = rb,
                .closed = false,
            };
            return q;
        }

        pub fn deinit(self: *Self, alloc: std.mem.Allocator) void {
            self.rb.deinit(alloc);
            alloc.destroy(self);
        }

        /// write at tail
        pub fn push(self: *Self, item: T) error{ Closed, Full }!void {
            if (self.closed) return error.Closed;
            try self.rb.push(item);
        }

        /// pop at head
        pub fn pop(self: *Self) ?T {
            return self.rb.pop();
        }

        pub fn isFull(self: *Self) bool {
            return self.rb.isFull();
        }

        pub fn isEmpty(self: *Self) bool {
            return self.rb.isEmpty();
        }

        pub fn capacity(self: *Self) usize {
            return self.rb.capacity();
        }

        pub fn close(self: *Self) void {
            self.closed = true;
        }
    };
}

test "create queue" {
    const alloc = std.testing.allocator;
    const q = try NonBlockingQueue([]u8).init(16, alloc);
    q.deinit(alloc);
}

test "single thread" {
    const alloc = std.testing.allocator;
    const buffer = try NonBlockingQueue([]const u8).init(16, alloc);
    defer buffer.deinit(alloc);

    const item1: []const u8 = "foo";
    const item2: []const u8 = "bar";

    try buffer.push(item1);
    try buffer.push(item2);
    const e1 = buffer.pop() orelse {
        return error.EmptyBuffer;
    };
    const e2 = buffer.pop() orelse {
        return error.EmptyBuffer;
    };

    // fifo ordering
    try std.testing.expectEqual("foo", e1);
    try std.testing.expectEqual("bar", e2);
}

test "close queue" {
    const alloc = std.testing.allocator;
    const buffer = try NonBlockingQueue([]const u8).init(16, alloc);
    defer buffer.deinit(alloc);

    const item1: []const u8 = "foo";
    const item2: []const u8 = "bar";

    try buffer.push(item1);
    buffer.close();
    try std.testing.expectError(error.Closed, buffer.push(item2));

    const e1 = buffer.pop() orelse return error.Failed;
    try std.testing.expectEqualStrings("foo", e1);
    try std.testing.expect(buffer.pop() == null);
}
