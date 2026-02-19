const std = @import("std");

pub const Config = @This();

/// how we distribute edits across docs also matters
database_batch_size: u64,
total_batches: u64,
database_latency_ms: u64,
scenario: enum { IN_MEMORY, PIPELINE, EVENT_LOOP },
seed: u64,

pub const Scenario = struct {
    ptr: *anyopaque,
    // impl
    initFn: *const fn (ptr: *anyopaque, config: Config, alloc: std.mem.Allocator) anyerror!void,
    runFn: *const fn (ptr: *anyopaque, config: Config, alloc: std.mem.Allocator) anyerror!Metrics,
    deinitFn: *const fn (ptr: *anyopaque, alloc: std.mem.Allocator) void,

    pub fn init(self: Scenario, config: Config, alloc: std.mem.Allocator) !void {
        try self.initFn(self.ptr, config, alloc);
    }

    pub fn run(self: Scenario, config: Config, alloc: std.mem.Allocator) !Metrics {
        return self.runFn(self.ptr, config, alloc);
    }

    pub fn deinit(self: Scenario, alloc: std.mem.Allocator) void {
        self.deinitFn(self.ptr, alloc);
    }
};

/// performance metrics
pub const Metrics = struct {
    /// fetching edits + applying edits >= total time
    total_time_ms: u64,
    /// first phase is fetching edits
    fetch_latency_ms: u64,
    /// second phase is applying edits
    apply_latency_ms: u64,
};
