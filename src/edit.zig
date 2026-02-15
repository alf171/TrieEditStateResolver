const std = @import("std");
const Writer = std.io.Writer;

const PathEdit = @import("pathEdit.zig");

pub const Edit = @This();

pathEdits: []const PathEdit,
timestamp: i64,

pub fn print(self: Edit, writer: *Writer) !void {
    try writer.print("{{\"pathEdits\":[", .{});
    for (self.pathEdits, 0..) |edit, i| {
        try edit.print(writer);
        if (i != self.pathEdits.len - 1)
            try writer.print(",", .{});
    }
    try writer.print("]}}", .{});
}

pub fn free(self: Edit, alloc: std.mem.Allocator) void {
    for (self.pathEdits) |pathEdit| {
        alloc.free(pathEdit.value);
    }
    alloc.free(self.pathEdits);
}
