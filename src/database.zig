const std = @import("std");
const Edit = @import("edit.zig");
const Generator = @import("generator.zig");

// an simulated inmemory database
pub const Database = @This();

pub const PendingReq = struct {
    token: u64,
    ready_at_ns: i128,
};

pub const Completion = struct {
    token: u64,
    result: anyerror![]Edit,
};

database_latency_ms: u64,
generator: *Generator,
/// tells us where we are in batches
cursor: u64,

/// used for async only
// pending: cannot run yet
// ready_tasks: can run now
// completed: work is done

pub fn init(generator: *Generator, database_latency_ms: u64) Database {
    return .{ .generator = generator, .database_latency_ms = database_latency_ms, .cursor = 0 };
}

/// return next page of edits. caller must free them
pub fn nextPage(self: *Database, alloc: std.mem.Allocator) ![]Edit {
    std.Thread.sleep(1000000 * sampleLatencyMs(self.generator.prng.random(), self.database_latency_ms));
    const edits = try self.generator.next_batch(alloc);
    self.cursor += 1;
    return edits;
}

fn sampleLatencyMs(rng: std.Random, base_ms: u64) u64 {
    const p = rng.float(f64);
    const b = @as(f64, @floatFromInt(@max(base_ms, 1)));

    var ms_f: f64 = 0.0;
    // ~0.8x..1.2x
    if (p < 0.85) {
        const u = rng.float(f64);
        ms_f = 0.8 * b + 0.4 * u * b;
    }
    // ~1.5x..2.5x
    else if (p < 0.99) {
        const u = rng.float(f64);
        ms_f = 1.5 * b + u * b;
    }
    // 5x..8x
    else {
        const u = rng.float(f64);
        ms_f = 5 * b + 3 * u * b;
    }
    return @intFromFloat(ms_f);
}
