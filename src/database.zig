const std = @import("std");
const Edit = @import("edit.zig");
const Generator = @import("generator.zig");
// an simulated inmemory database
pub const Database = @This();

database_latency_ms: u64,
generator: *Generator,
/// tells us where we are in batches
cursor: u64,

pub fn init(generator: *Generator, database_latency_ms: u64) Database {
    return .{ .generator = generator, .database_latency_ms = database_latency_ms, .cursor = 0 };
}

/// return next page of edits. caller must free them
pub fn nextPage(self: *Database, alloc: std.mem.Allocator) ![]Edit {
    std.Thread.sleep(1000000 * self.database_latency_ms);
    const edits = try self.generator.next_batch(alloc);
    self.cursor += 1;
    return edits;
}
