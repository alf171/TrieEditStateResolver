const std = @import("std");
const Writer = std.io.Writer;

pub const EditAction = enum { PUT, DELETE };

/// support literal values and nested structs only
pub const Value = union(enum) {
    string: []const u8,
    object: []const Field,

    pub const Field = struct {
        key: []const u8,
        value: Value,
    };
};

pub const PutStruct = struct {
    path: []const u8,
    value: Value,
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
            try stdout.print("\"action\":\"{s}\",\"path\":\"{s}\",\"value\":", .{ "PUT", put.path });
            try printValue(put.value, stdout);
            try stdout.print("}}", .{});
        },
        .DELETE => |delete| {
            try stdout.print("{{", .{});
            try stdout.print("\"action\":\"{s}\",\"path\":\"{s}\"", .{ "DELETE", delete.path });
            try stdout.print("}}", .{});
        },
    }
}

fn printValue(value: Value, stdout: *Writer) !void {
    switch (value) {
        .string => |str| try stdout.print("\"{s}\"", .{str}),
        .object => |items| {
            try stdout.print("{{", .{});
            for (items, 0..) |entry, i| {
                if (i != 0) try stdout.print(",", .{});
                try stdout.print("\"{s}\":", .{entry.key});
                try printValue(entry.value, stdout);
            }
            try stdout.print("}}", .{});
        },
    }
}

pub fn initPut(path: []const u8, value: Value) PathEdit {
    return .{ .PUT = .{ .path = path, .value = value } };
}

pub fn initDelete(path: []const u8) PathEdit {
    return .{ .DELETE = .{ .path = path } };
}

pub fn deinit(self: PathEdit, alloc: std.mem.Allocator) void {
    switch (self) {
        .PUT => |put| deinitValue(put.value, alloc),
        .DELETE => return,
    }
}

fn deinitValue(value: Value, alloc: std.mem.Allocator) void {
    switch (value) {
        .string => |s| alloc.free(s),
        .object => |fields| {
            for (fields) |field| {
                alloc.free(field.key);
                deinitValue(field.value, alloc);
            }
            alloc.free(fields);
        },
    }
}

test "put pathEdit" {
    const edit = initPut("a", .{ .string = "b" });

    switch (edit) {
        .PUT => |put| {
            try std.testing.expectEqual("a", put.path);
            try std.testing.expectEqual("b", put.value.string);
        },
        .DELETE => return error.TestUnexpectedResult,
    }
}
test "nested put pathEdit" {
    const edit = initPut("a", .{
        .object = &[_]Value.Field{
            .{ .key = "b", .value = .{ .string = "1" } },
            .{ .key = "c", .value = .{ .string = "2" } },
        },
    });

    switch (edit) {
        .PUT => |put| {
            try std.testing.expectEqual("a", put.path);
            try std.testing.expect(put.value == .object);
        },
        .DELETE => return error.TestUnexpectedResult,
    }
}

test "delete pathEdit" {
    const edit = initDelete("a");

    switch (edit) {
        .DELETE => |del| {
            try std.testing.expectEqualStrings("a", del.path);
        },
        .PUT => return error.TestUnexpectedResult,
    }
}
