const std = @import("std");
const Writer = std.io.Writer;

pub const EditAction = enum { PUT, DELETE };

pub const PutStruct = struct {
    path: []const u8,
    value: []const u8,
};

pub const DeleteStruct = struct { path: []const u8 };

pub const PathEdit = union(EditAction) {
    PUT: PutStruct,
    DELETE: DeleteStruct,
};

pub fn print(self: PathEdit, stdout: *Writer) !void {
    switch (self) {
        .PUT => |put| {
            try stdout.print("{{", .{});
            try stdout.print("\"action\":\"{s}\",\"path\":\"{s}\",\"value\":\"{s}\"", .{ "PUT", put.path, put.value });
            try stdout.print("}}", .{});
        },
        .DELETE => |delete| {
            try stdout.print("{{", .{});
            try stdout.print("\"action\":\"{s}\",\"path\":\"{s}\"", .{ "DELETE", delete.path });
            try stdout.print("}}", .{});
        },
    }
}

pub fn deinit(self: PathEdit, alloc: std.mem.Allocator) void {
    switch (self) {
        .PUT => |put| alloc.free(put.value),
        .DELETE => return,
    }
}

test "put pathEdit" {
    const edit = PathEdit{ .PUT = .{ .path = "a", .value = "b" } };

    switch (edit) {
        .PUT => |put| {
            try std.testing.expectEqual("a", put.path);
            try std.testing.expectEqual("b", put.value);
        },
        .DELETE => return error.TestUnexpectedResult,
    }
}

test "delete pathEdit" {
    const edit = PathEdit{ .DELETE = .{ .path = "a" } };

    switch (edit) {
        .DELETE => |del| {
            try std.testing.expectEqualStrings("a", del.path);
        },
        .PUT => return error.TestUnexpectedResult,
    }
}
