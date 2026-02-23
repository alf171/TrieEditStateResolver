const std = @import("std");
const Config = @import("config.zig").Config;
const Database = @import("../database.zig");
const Document = @import("../document.zig");
const Generator = @import("../generator.zig");
const LoadTest = @import("../loadtest/config.zig");
const Set = @import("../set.zig");

const Metrics = Config.Metrics;
const Timer = std.time.Timer;
const ns_per_ms = 1_000_000;

pub const EventLoopScenario = @This();

document: Document,
/// we will use async version of database
database: *Database,

pub fn initFn(ptr: *anyopaque, config: Config, alloc: std.mem.Allocator) anyerror!void {
    const self: *EventLoopScenario = @ptrCast(@alignCast(ptr));
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
    const timestamp = std.time.milliTimestamp();
    self.document = try Document.init(timestamp, alloc);
}

pub fn runFn(ptr: *anyopaque, config: Config, alloc: std.mem.Allocator) anyerror!Metrics {
    const self: *EventLoopScenario = @ptrCast(@alignCast(ptr));

    var submitted: u64 = 0;
    var completed: u64 = 0;
    var curToken: u64 = 0;

    var total_timer = try Timer.start();
    while (self.database.cursor < config.total_batches) {
        if (submitted == completed) {
            try self.database.asyncNextPage(curToken);
            submitted += 1;
            curToken += 1;
        }

        try self.database.tick(alloc);

        const completion = self.database.pollForCompletion() catch |err| {
            switch (err) {
                error.NotCompleted => continue,
            }
        };
        const edits = try completion.result;
        for (edits) |edit| {
            try self.document.applyEdit(edit, alloc);
            edit.free(alloc);
        }
        alloc.free(edits);
        completed += 1;
    }
    const total_time_ms = total_timer.read() / ns_per_ms;

    return .{ .fetch_latency_ms = 0, .apply_latency_ms = 0, .total_time_ms = total_time_ms };
}

pub fn deinitFn(ptr: *anyopaque, alloc: std.mem.Allocator) void {
    const self: *EventLoopScenario = @ptrCast(@alignCast(ptr));
    self.database.deinit(alloc);
    self.database.generator.deinit(alloc);
    self.document.free(alloc);
}

test "create event loop" {
    const alloc = std.testing.allocator;
    const config = LoadTest.Config{
        .database_latency_ms = 0,
        .total_batches = 5,
        .database_batch_size = 2,
        .scenario = .EVENT_LOOP,
        .seed = 0,
    };
    var scenario_impl = EventLoopScenario{ .database = undefined, .document = undefined };
    const scenario = LoadTest.Config.Scenario{
        .ptr = &scenario_impl,
        .initFn = EventLoopScenario.initFn,
        .runFn = EventLoopScenario.runFn,
        .deinitFn = EventLoopScenario.deinitFn,
    };

    try scenario.init(config, alloc);
    defer scenario.deinit(alloc);
}
