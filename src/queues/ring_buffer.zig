const std = @import("std");

pub fn RingBuffer(comptime T: type) type {
    return struct {
        const Self = @This();
        data: []?T,
        head: usize = 0,
        tail: usize = 0,
        count: usize = 0,

        pub fn init(cap: usize, alloc: std.mem.Allocator) !*Self {
            std.debug.assert(cap > 0);
            const buffer = try alloc.create(Self);
            errdefer alloc.destroy(buffer);
            const data = try alloc.alloc(?T, cap);
            @memset(data, null);
            buffer.* = Self{
                .data = data,
                .head = 0,
                .tail = 0,
                .count = 0,
            };
            return buffer;
        }

        pub fn deinit(self: *Self, alloc: std.mem.Allocator) void {
            alloc.free(self.data);
            alloc.destroy(self);
        }

        /// write at tail
        pub fn push(self: *Self, item: T) error{Full}!void {
            if (self.isFull()) {
                return error.Full;
            }
            self.data[self.tail] = item;
            self.tail = (self.tail + 1) % self.data.len;
            self.count += 1;
        }

        /// pop at head
        pub fn pop(self: *Self) ?T {
            const item = self.data[self.head];
            self.data[self.head] = null;
            if (item == null) {
                return null;
            }
            self.head = (self.head + 1) % self.data.len;
            self.count -= 1;
            return item;
        }

        pub fn isFull(self: *Self) bool {
            return self.count == self.data.len;
        }

        pub fn isEmpty(self: *Self) bool {
            return self.count == 0;
        }

        pub fn capacity(self: *Self) usize {
            return self.data.len;
        }
    };
}

test "create queue" {
    const alloc = std.testing.allocator;
    const q = try RingBuffer([]u8).init(16, alloc);
    q.deinit(alloc);
}

test "single thread" {
    const alloc = std.testing.allocator;
    const buffer = try RingBuffer([]const u8).init(16, alloc);
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
