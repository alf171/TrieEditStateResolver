const std = @import("std");

const Config = @import("config.zig").Config;
const Database = @import("../database.zig");
const Document = @import("../document.zig");
const Generator = @import("../generator.zig");
const Set = @import("../set.zig");

const Metrics = Config.Metrics;
const Timer = std.time.Timer;
const ns_per_ms = 1_000_000;

pub const InMemoryScenario = @This();

document: Document,
database: *Database,

pub fn initFn(ptr: *anyopaque, config: Config, alloc: std.mem.Allocator) anyerror!void {
    const self: *InMemoryScenario = @ptrCast(@alignCast(ptr));
    const paths = try Set.of(&.{ "a/b", "a/c", "a/d", "a/e" }, alloc);
    const editGenerator = try alloc.create(Generator.EditGenerator);
    editGenerator.* = Generator.EditGenerator{
        .batch_size = config.database_batch_size,
        .min_value_len = 10,
        .max_value_len = 20,
        .path_edits_per_edit = 2,
        .prng = std.Random.DefaultPrng.init(config.seed),
        .set_of_paths = paths.data,
    };

    self.database = try Database.init(editGenerator, config.database_latency_ms, alloc);
    self.document = try Document.init(alloc);
}

pub fn runFn(ptr: *anyopaque, config: Config, alloc: std.mem.Allocator) anyerror!Metrics {
    const self: *InMemoryScenario = @ptrCast(@alignCast(ptr));

    var fetch_ns: u64 = 0;
    var apply_ns: u64 = 0;
    var total_timer = try Timer.start();
    while (self.database.cursor < config.total_batches) {
        // phase 1: fetch edits
        var fetch_timer = try Timer.start();
        const edits = try self.database.nextPage(alloc);
        fetch_ns += fetch_timer.read();
        // phase 2: apply edits
        var apply_timer = try Timer.start();
        for (edits) |edit| {
            try self.document.applyEdit(edit, alloc);
            edit.free(alloc);
        }
        apply_ns += apply_timer.read();
        alloc.free(edits);
    }
    const total_time_ms = total_timer.read() / ns_per_ms;
    const fetch_latency_ms = fetch_ns / ns_per_ms;
    const apply_latency_ms = apply_ns / ns_per_ms;

    return .{ .fetch_latency_ms = fetch_latency_ms, .apply_latency_ms = apply_latency_ms, .total_time_ms = total_time_ms };
}

pub fn deinitFn(ptr: *anyopaque, alloc: std.mem.Allocator) void {
    const self: *InMemoryScenario = @ptrCast(@alignCast(ptr));
    self.database.generator.deinit(alloc);
    self.database.deinit(alloc);
    self.document.free(alloc);
}
