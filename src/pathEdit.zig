const std = @import("std");
const Writer = std.io.Writer;

pub const PathEdit = @This();

path: []const u8,
value: []const u8,

pub fn print(self: PathEdit, stdout: *Writer) !void {
    try stdout.print("{{", .{});
    try stdout.print("\"path\":\"{s}\",\"value\":\"{s}\"", .{ self.path, self.value });
    try stdout.print("}}", .{});
}
