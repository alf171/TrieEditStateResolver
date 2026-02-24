const std = @import("std");
const Edit = @import("edit.zig");
const PathEdit = @import("pathEdit.zig").PathEdit;
const EditAction = PathEdit.EditAction;
const Set = @import("set.zig");

/// assume we are generating edits for a single document
pub const EditGenerator = @This();

/// reproduce randomness
prng: std.Random.DefaultPrng,
/// how many edits to generate per batch
batch_size: u64,
/// average pathEdits per edit
path_edits_per_edit: u64,
/// fill values with random chars between [min, max]
min_value_len: u64,
max_value_len: u64,
/// what paths to apply edits across
set_of_paths: std.StringArrayHashMap(void),
timestamp_order: enum { IN_ORDER, RANDOM },
actions: enum { PUT, PUT_AND_DELETE },

pub fn default_init(seed: u64, alloc: std.mem.Allocator) !*EditGenerator {
    const paths = try Set.of(&.{ "a/b", "a/c", "a/d", "a/e" }, alloc);
    const gen = try alloc.create(EditGenerator);
    gen.* = EditGenerator{
        .batch_size = 10,
        .min_value_len = 10,
        .max_value_len = 20,
        .path_edits_per_edit = 2,
        .prng = std.Random.DefaultPrng.init(seed),
        .set_of_paths = paths.data,
        .timestamp_order = .IN_ORDER,
        .actions = .PUT,
    };
    return gen;
}

pub fn deinit(self: *EditGenerator, alloc: std.mem.Allocator) void {
    self.set_of_paths.deinit();
    alloc.destroy(self);
}

pub fn next_batch(self: *EditGenerator, alloc: std.mem.Allocator) ![]Edit {
    const edits = try alloc.alloc(Edit, self.batch_size);
    for (0..self.batch_size) |batch_index| {
        const pathEdits = try alloc.alloc(PathEdit, self.path_edits_per_edit);
        for (0..self.path_edits_per_edit) |path_edit_idx| {
            const pathEdit = switch (self.actions) {
                .PUT => PathEdit{ .PUT = .{ .path = self.random_path(), .value = try self.random_value(alloc) } },
                .PUT_AND_DELETE => blk: {
                    const random = self.prng.random();
                    if (random.boolean()) {
                        break :blk PathEdit{ .PUT = .{ .path = self.random_path(), .value = try self.random_value(alloc) } };
                    } else {
                        break :blk PathEdit{ .DELETE = .{ .path = self.random_path() } };
                    }
                },
            };
            pathEdits[@intCast(path_edit_idx)] = pathEdit;
        }
        const timestamp = switch (self.timestamp_order) {
            .IN_ORDER => std.time.microTimestamp(),
            .RANDOM => blk: {
                const random = self.prng.random();
                const now = std.time.microTimestamp();
                const one_day_us: i64 = 24 * 60 * 60 * 1_000_000;
                break :blk random.intRangeAtMost(i64, now - one_day_us, now + one_day_us);
            },
        };
        edits[batch_index] = Edit{
            .pathEdits = pathEdits,
            .timestamp = timestamp,
        };
    }
    return edits;
}

fn random_path(self: *EditGenerator) []const u8 {
    const keys = self.set_of_paths.keys();
    std.debug.assert(keys.len > 0);
    const random = self.prng.random();
    const idx = random.intRangeAtMost(usize, 0, keys.len - 1);
    return keys[idx];
}

fn random_value(self: *EditGenerator, alloc: std.mem.Allocator) ![]const u8 {
    const random = self.prng.random();
    const length = random.intRangeAtMost(u64, self.min_value_len, self.max_value_len);
    const buf = try alloc.alloc(u8, length);
    const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789";
    for (buf) |*ch| {
        const idx = random.intRangeAtMost(u8, 0, alphabet.len - 1);
        ch.* = alphabet[idx];
    }
    return buf;
}

test "ordered edits" {
    const alloc = std.testing.allocator;
    const gen = try default_init(0, alloc);
    defer gen.deinit(alloc);
    const edits = try gen.next_batch(alloc);
    var last_timestamp: i64 = std.math.minInt(i64);
    for (edits) |edit| {
        defer edit.free(alloc);
        if (edit.timestamp < last_timestamp) return error.WrongOrder;
        last_timestamp = edit.timestamp;
    }
    alloc.free(edits);
}
