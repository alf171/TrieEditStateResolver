const std = @import("std");

pub const Set = @This();

data: std.StringArrayHashMap(void),

pub fn deinit(self: *Set) void {
    self.data.deinit();
}

pub fn of(strings: []const []const u8, alloc: std.mem.Allocator) !Set {
    var data = std.StringArrayHashMap(void).init(alloc);
    for (strings) |string| {
        try data.put(string, {});
    }
    return Set{ .data = data };
}
