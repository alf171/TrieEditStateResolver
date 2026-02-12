const std = @import("std");
const PathEdit = @import("pathEdit.zig");
const Edit = @import("edit.zig");
const Document = @import("document.zig");
const EditGenerator = @import("generator.zig");

const Writer = std.io.Writer;

pub fn main() !void {
    var stdout_buffer: [1024]u8 = undefined;
    var stdout_writer = std.fs.File.stdout().writer(&stdout_buffer);
    const stdout = &stdout_writer.interface;

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    // NOTE: write a utility to do this in a single line
    var set = std.StringArrayHashMap(void).init(alloc);
    defer set.deinit();
    try set.put("a/b", {});
    try set.put("a/c", {});
    try set.put("a/d", {});
    try set.put("a/e", {});

    const config = EditGenerator{
        .batch_size = 10,
        .min_value_len = 10,
        .max_value_len = 20,
        .path_edits_per_edit = 2,
        .seed = 0,
        .set_of_paths = set,
    };
    const edits = try config.next_batch(alloc);
    var doc = try Document.init(alloc);
    defer doc.free(alloc);
    // TODO: improve memory model
    for (edits) |edit| {
        try doc.applyEdit(edit, alloc);
        for (edit.pathEdits) |pathEdit| {
            defer alloc.free(pathEdit.value);
        }
        defer alloc.free(edit.pathEdits);
    }
    defer alloc.free(edits);

    try doc.print(stdout);
    try stdout.flush();
}

fn doc_example() !void {
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
