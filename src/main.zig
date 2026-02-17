const std = @import("std");
const BatchQueue = @import("loadtest/batch_queue.zig");
const PathEdit = @import("pathEdit.zig");
const Edit = @import("edit.zig");
const Document = @import("document.zig");
const EditGenerator = @import("generator.zig");
const LoadTest = @import("loadtest/config.zig");
const InMemory = @import("loadtest/in_memory.zig");
const Pipeline = @import("loadtest/pipeline.zig");
const Set = @import("set.zig");

const Writer = std.io.Writer;

pub fn main() !void {
    try in_memory_load_test();
    try pipeline_load_test();
}

fn pipeline_load_test() !void {
    var stdout_buffer: [1024]u8 = undefined;
    var stdout_writer = std.fs.File.stdout().writer(&stdout_buffer);
    const stdout = &stdout_writer.interface;
    var stderr_buffer: [256]u8 = undefined;
    var stderr_writer = std.fs.File.stderr().writer(&stderr_buffer);
    const stderr = &stderr_writer.interface;

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();
    const config = LoadTest.Config{
        .database_latency_ms = 5,
        .total_batches = 50,
        .database_batch_size = 100,
        .scenario = .PIPELINE,
        .seed = 0,
    };

    var scenario_impl = Pipeline{ .database = undefined, .document = undefined, .queue = undefined };
    const scenario = LoadTest.Config.Scenario{
        .ptr = &scenario_impl,
        .initFn = Pipeline.initFn,
        .runFn = Pipeline.runFn,
        .deinitFn = Pipeline.deinitFn,
    };

    try scenario.init(config, alloc);
    defer scenario.deinit(alloc);

    const metrics = try scenario.run(config, alloc);
    try scenario_impl.document.print(stdout);
    try stdout.print("\n", .{});
    try stdout.flush();
    try stderr.print("[PIPELINE] total latency: {}ms, fetch latency {}ms, apply latency {}ms", .{ metrics.total_time_ms, metrics.fetch_latency_ms, metrics.apply_latency_ms });
    try stderr.print("\n", .{});
    try stderr.flush();
}

fn in_memory_load_test() !void {
    var stdout_buffer: [1024]u8 = undefined;
    var stdout_writer = std.fs.File.stdout().writer(&stdout_buffer);
    const stdout = &stdout_writer.interface;
    var stderr_buffer: [256]u8 = undefined;
    var stderr_writer = std.fs.File.stderr().writer(&stderr_buffer);
    const stderr = &stderr_writer.interface;

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();
    const config = LoadTest.Config{
        .database_latency_ms = 5,
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
    try scenario_impl.document.print(stdout);
    try stdout.print("\n", .{});
    try stdout.flush();
    try stderr.print("[IN_MEMORY] total latency: {}ms, fetch latency {}ms, apply latency {}ms", .{ metrics.total_time_ms, metrics.fetch_latency_ms, metrics.apply_latency_ms });
    try stderr.print("\n", .{});
    try stderr.flush();
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
