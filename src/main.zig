const std = @import("std");
const PathEdit = @import("pathEdit.zig");
const Edit = @import("edit.zig");
const Document = @import("document.zig");

const Writer = std.io.Writer;

pub fn main() !void {
    var stdout_buffer: [1024]u8 = undefined;
    var stdout_writer = std.fs.File.stdout().writer(&stdout_buffer);
    const stdout = &stdout_writer.interface;

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    var doc = try Document.init(alloc);
    defer doc.free(alloc);
    const pathEdit1 = PathEdit{ .path = "a/b", .value = "foo" };
    const pathEdit2 = PathEdit{ .path = "a/c", .value = "bar" };
    const edit = Edit{
        .pathEdits = &[_]PathEdit{ pathEdit1, pathEdit2 },
        .timestamp = 0,
    };
    try doc.applyEdit(edit, alloc);
    try doc.print(stdout);
    try stdout.flush();
}
