const std = @import("std");
const PathEdit = @import("pathEdit.zig");
const Edit = @import("edit.zig");
const Document = @import("document.zig");
const EditGenerator = @import("generator.zig");
const LoadTest = @import("loadtest/config.zig");
const InMemory = @import("loadtest/in_memory.zig");
const Set = @import("set.zig");

const Writer = std.io.Writer;

pub fn main() !void {
    var stdout_buffer: [1024]u8 = undefined;
    var stdout_writer = std.fs.File.stdout().writer(&stdout_buffer);
    const stdout = &stdout_writer.interface;

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    const config = LoadTest.Config{
        .database_latency_ms = 10,
        .total_batches = 50,
        .database_batch_size = 100,
        .scenario = .IN_MEMORY,
        .seed = 0,
    };

    var scenario_impl = InMemory{ .database = undefined, .document = undefined };
    const scenario = LoadTest.Config.Scenario{
        .ptr = &scenario_impl,
        .initFn = InMemory.initFn,
        .runFn = InMemory.runFn,
        .deinitFn = InMemory.deinitFn,
    };

    try scenario.init(config, alloc);
    defer scenario.deinit(alloc);

    const metrics = try scenario.run(config, alloc);
    try stdout.print("total latency: {}ms, fetch latency {}ms, apply latency {}ms", .{ metrics.total_time_ms, metrics.fetch_latency_ms, metrics.apply_latency_ms });
    try stdout.flush();
}

fn generator_example() !void {
    var stdout_buffer: [1024]u8 = undefined;
    var stdout_writer = std.fs.File.stdout().writer(&stdout_buffer);
    const stdout = &stdout_writer.interface;

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    var editGenerator = try EditGenerator.default_init(alloc);
    defer editGenerator.deinit();

    const edits = try editGenerator.next_batch(alloc);

    var doc = try Document.init(alloc);
    defer doc.free(alloc);
    for (edits) |edit| {
        try doc.applyEdit(edit, alloc);
        defer edit.free(alloc);
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
