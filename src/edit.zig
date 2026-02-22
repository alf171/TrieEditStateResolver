const std = @import("std");
const Writer = std.io.Writer;

const PathEditModule = @import("pathEdit.zig");
const PathEdit = @import("pathEdit.zig").PathEdit;

pub const Edit = @This();

pathEdits: []const PathEdit,
timestamp: i64,

pub fn print(self: Edit, writer: *Writer) !void {
    try writer.print("{{\"pathEdits\":[", .{});
    for (self.pathEdits, 0..) |pathEdit, i| {
        try pathEdit.print(writer);
        if (i != self.pathEdits.len - 1)
            try writer.print(",", .{});
    }
    try writer.print("]}}", .{});
}

pub fn free(self: Edit, alloc: std.mem.Allocator) void {
    for (self.pathEdits) |pathEdit| {
        PathEditModule.deinit(pathEdit, alloc);
    }
    alloc.free(self.pathEdits);
}
