const std = @import("std");
const Time = @import("time.zig");
const Edit = @import("edit.zig");
const PathEdit = @import("pathEdit.zig").PathEdit;
const EditAction = @import("pathEdit.zig").EditAction;
const PutStruct = @import("pathEdit.zig").PutStruct;

pub const Document = @This();

const UUID_LENGTH = 16;

const Node = struct {
    timestamp: i64,
    data: union(enum) {
        children: std.StringHashMap(*Node),
        value: []u8,
        tombstone: bool,
    },

    pub fn initChildren(ts: i64, alloc: std.mem.Allocator) !*Node {
        const node = try alloc.create(Node);
        node.* = .{
            .data = .{ .children = std.StringHashMap(*Node).init(alloc) },
            .timestamp = ts,
        };
        return node;
    }

    pub fn initValue(value: []const u8, ts: i64, alloc: std.mem.Allocator) !*Node {
        const node = try alloc.create(Node);
        node.* = .{
            .data = .{ .value = try alloc.dupe(u8, value) },
            .timestamp = ts,
        };
        return node;
    }

    pub fn initTombstone(ts: i64, alloc: std.mem.Allocator) !*Node {
        const node = try alloc.create(Node);
        node.* = .{
            .data = .{ .tombstone = true },
            .timestamp = ts,
        };
        return node;
    }
};

root: *Node,
/// document will own these for nows not json blob
uuid: []u8,
lastUpdatedTimestamp: i64,
editCount: u64,

pub fn init(ts: i64, alloc: std.mem.Allocator) !Document {
    const uuid = try alloc.alloc(u8, UUID_LENGTH);
    const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789";
    for (uuid) |*ch| {
        const idx = std.crypto.random.intRangeAtMost(u8, 0, alphabet.len - 1);
        ch.* = alphabet[idx];
    }
    return .{ .root = try Node.initChildren(ts, alloc), .uuid = uuid, .lastUpdatedTimestamp = ts, .editCount = 0 };
}

pub fn free(self: Document, alloc: std.mem.Allocator) void {
    alloc.free(self.uuid);
    freeNode(self.root, alloc);
}

fn freeNode(node: *Node, alloc: std.mem.Allocator) void {
    switch (node.*.data) {
        .children => |*map| {
            var it = map.iterator();
            while (it.next()) |cur| {
                const child = cur.value_ptr.*;
                freeNode(child, alloc);
            }
            map.deinit();
        },
        .value => |v| {
            alloc.free(v);
        },
        .tombstone => {},
    }
    alloc.destroy(node);
}

pub fn print(self: Document, writer: *std.io.Writer, alloc: std.mem.Allocator) !void {
    try writer.print("{{\"id\":\"{s}\",", .{self.uuid});
    const timestamp = try Time.iso8601format(self.lastUpdatedTimestamp, alloc);
    defer alloc.free(timestamp);
    try writer.print("\"lastUpdatedTimestamp\":\"{s}\",", .{timestamp});
    try writer.print("\"editCount\":\"{d}\",\"data\":", .{self.editCount});
    try printNode(self.root, writer);
    try writer.print("}}", .{});
}

fn printNode(node: *const Node, writer: *std.io.Writer) !void {
    switch (node.*.data) {
        .value => |v| {
            try writer.print("\"{s}\"", .{v});
        },
        .children => |map| {
            try writer.print("{{", .{});
            var it = map.iterator();
            var first = true;
            while (it.next()) |entry| {
                if (!first) try writer.print(",", .{});
                first = false;
                try writer.print("\"{s}\":", .{entry.key_ptr.*});
                try printNode(entry.value_ptr.*, writer);
            }
            try writer.print("}}", .{});
        },
        .tombstone => return,
    }
}

pub fn toJson(node: *const Node, alloc: std.mem.Allocator) ![]u8 {
    // _ = alloc;
    var aw = std.io.Writer.Allocating.init(alloc);
    defer aw.deinit();

    try printNode(node, &aw.writer);
    return try aw.toOwnedSlice();
}

/// apply an edit onto a document
/// does not consider timestamp atm
pub fn applyEdit(self: *Document, edit: Edit, alloc: std.mem.Allocator) !void {
    self.editCount += 1;
    for (edit.pathEdits) |pathEdit| {
        try switch (pathEdit) {
            EditAction.PUT => |put| applyEditPut(self, put, edit.timestamp, alloc),
            EditAction.DELETE => |del| applyEditDelete(self, del.path, edit.timestamp, alloc),
        };
    }
    if (edit.timestamp > self.lastUpdatedTimestamp) {
        self.lastUpdatedTimestamp = edit.timestamp;
    }
}

fn applyEditDelete(self: *Document, path: []const u8, ts: i64, alloc: std.mem.Allocator) !void {
    var parts = std.mem.splitSequence(u8, path, "/");
    var previous_nodes = &self.root.data.children;
    var next_part = parts.next() orelse return;
    while (true) {
        const part = next_part;
        const next = parts.next();
        const is_last = (next == null);

        if (is_last) {
            if (previous_nodes.get(part)) |cur| {
                if (cur.timestamp >= ts) return;
                if (previous_nodes.fetchRemove(part)) |removed| {
                    freeNode(removed.value, alloc);
                }
            }
            const tombstone = try Node.initTombstone(ts, alloc);
            try previous_nodes.put(part, tombstone);
            return;
        }

        const existing = previous_nodes.get(part) orelse return;
        switch (existing.*.data) {
            .children => |*children| previous_nodes = children,
            .value => return,
            .tombstone => return,
        }

        next_part = next.?;
    }
}

fn applyEditPut(self: *Document, pathEdit: PutStruct, ts: i64, alloc: std.mem.Allocator) !void {
    var parts = std.mem.splitSequence(u8, pathEdit.path, "/");
    var previous_nodes = &self.root.data.children;
    var next_part = parts.next() orelse return;
    while (true) {
        const part = next_part;
        const next = parts.next();
        const is_last = (next == null);

        if (is_last) {
            if (previous_nodes.get(part)) |cur| {
                if (cur.timestamp >= ts) return;
                if (previous_nodes.fetchRemove(part)) |removed| {
                    freeNode(removed.value, alloc);
                }
            }
            const leaf = try Node.initValue(pathEdit.value, ts, alloc);
            try previous_nodes.put(part, leaf);
            return;
        }

        const existing = previous_nodes.get(part);
        if (existing) |ptr| {
            switch (ptr.*.data) {
                .children => |*children| {
                    previous_nodes = children;
                },
                .value => {
                    if (ptr.timestamp >= ts) return;
                    freeNode(ptr, alloc);
                    const new_node = try Node.initChildren(ts, alloc);
                    try previous_nodes.put(part, new_node);
                    previous_nodes = &new_node.data.children;
                },
                .tombstone => {
                    if (ptr.timestamp >= ts) return;
                    ptr.data = .{ .children = std.StringHashMap(*Node).init(alloc) };
                    ptr.timestamp = ts;
                    previous_nodes = &ptr.data.children;
                },
            }
        } else {
            const child = try Node.initChildren(ts, alloc);
            try previous_nodes.put(part, child);
            previous_nodes = &child.data.children;
        }
        next_part = next.?;
    }
}

/// ownership of string is given to caller
pub fn get(self: *Document, path: []const u8, alloc: std.mem.Allocator) error{ NotFound, NotImplemented, OutOfMemory, WriteFailed }![]const u8 {
    var parts = std.mem.splitSequence(u8, path, "/");
    var curBlob: *const Node = self.root;
    while (parts.next()) |part| {
        switch (curBlob.*.data) {
            .children => |map| {
                curBlob = map.get(part) orelse return error.NotFound;
            },
            .tombstone => return error.NotFound,
            // get("a/b") on {"a"}
            .value => return error.NotFound,
        }
    }
    return switch (curBlob.*.data) {
        // get("a") on {"a/b/c"} => b: { c: v }
        .children => try toJson(curBlob, alloc),
        .value => |value| alloc.dupe(u8, value),
        .tombstone => error.NotFound,
    };
}

test "put edit on document" {
    const alloc = std.testing.allocator;
    var doc = try Document.init(0, alloc);
    defer doc.free(alloc);

    const pathEdits = [_]PathEdit{.{ .PUT = .{
        .path = "a/b",
        .value = "foo",
    } }};
    const edit = Edit{ .pathEdits = pathEdits[0..], .timestamp = 0 };
    try doc.applyEdit(edit, alloc);
    const abstring = try doc.get("a/b", alloc);
    defer alloc.free(abstring);
    try std.testing.expectEqualStrings("foo", abstring);
    const astring = try doc.get("a", alloc);
    defer alloc.free(astring);
    try std.testing.expectEqualStrings("{\"b\":\"foo\"}", astring);
    try std.testing.expectEqual(0, doc.root.timestamp);
}

test "delete path from document" {
    const alloc = std.testing.allocator;
    var doc = try Document.init(0, alloc);
    defer doc.free(alloc);

    const pathEdits = [_]PathEdit{
        .{ .PUT = .{ .path = "a", .value = "foo" } },
    };
    const deletePathEdits = [_]PathEdit{
        .{ .DELETE = .{ .path = "a" } },
    };
    const addEdit = Edit{ .pathEdits = pathEdits[0..], .timestamp = 0 };
    const removeEdit = Edit{ .pathEdits = deletePathEdits[0..], .timestamp = 1 };
    try doc.applyEdit(addEdit, alloc);
    try doc.applyEdit(removeEdit, alloc);
    try std.testing.expectError(error.NotFound, doc.get("a", alloc));
    try std.testing.expectEqual(1, doc.lastUpdatedTimestamp);
}

test "timestamp clash" {
    const alloc = std.testing.allocator;
    var doc = try Document.init(0, alloc);
    defer doc.free(alloc);

    const pe1 = [_]PathEdit{
        .{ .PUT = .{ .path = "a", .value = "foo" } },
    };
    const pe2 = [_]PathEdit{
        .{ .PUT = .{ .path = "a", .value = "bar" } },
    };
    const pe3 = [_]PathEdit{
        .{ .DELETE = .{ .path = "a" } },
    };
    const edit1 = Edit{ .pathEdits = pe1[0..], .timestamp = 2 };
    const edit2 = Edit{ .pathEdits = pe2[0..], .timestamp = 1 };
    const edit3 = Edit{ .pathEdits = pe3[0..], .timestamp = 0 };
    try doc.applyEdit(edit1, alloc);
    try doc.applyEdit(edit2, alloc);
    try doc.applyEdit(edit3, alloc);

    const astring = try doc.get("a", alloc);
    defer alloc.free(astring);
    try std.testing.expectEqualStrings("foo", astring);
}

test "tombstone: newer delete first" {
    const alloc = std.testing.allocator;
    var doc = try Document.init(0, alloc);
    defer doc.free(alloc);

    const pe1 = [_]PathEdit{
        .{ .DELETE = .{ .path = "a" } },
    };

    const pe2 = [_]PathEdit{
        .{ .PUT = .{ .path = "a", .value = "foo" } },
    };

    const edit1 = Edit{ .pathEdits = pe1[0..], .timestamp = 1 };
    const edit2 = Edit{ .pathEdits = pe2[0..], .timestamp = 0 };
    try doc.applyEdit(edit1, alloc);
    try doc.applyEdit(edit2, alloc);

    try std.testing.expectError(error.NotFound, doc.get("a", alloc));
}

test "tombstone: ancestor deny" {
    const alloc = std.testing.allocator;
    var doc = try Document.init(0, alloc);
    defer doc.free(alloc);

    const pe1 = [_]PathEdit{
        .{ .DELETE = .{ .path = "a" } },
    };

    const pe2 = [_]PathEdit{
        .{ .PUT = .{ .path = "a/b", .value = "foo" } },
    };

    const edit1 = Edit{ .pathEdits = pe1[0..], .timestamp = 1 };
    const edit2 = Edit{ .pathEdits = pe2[0..], .timestamp = 0 };
    try doc.applyEdit(edit1, alloc);
    try doc.applyEdit(edit2, alloc);

    try std.testing.expectError(error.NotFound, doc.get("a/b", alloc));
}

test "tombstone: ancestor allow" {
    const alloc = std.testing.allocator;
    var doc = try Document.init(0, alloc);
    defer doc.free(alloc);

    const pe1 = [_]PathEdit{
        .{ .DELETE = .{ .path = "a" } },
    };

    const pe2 = [_]PathEdit{
        .{ .PUT = .{ .path = "a/b", .value = "foo" } },
    };

    const edit1 = Edit{ .pathEdits = pe1[0..], .timestamp = 0 };
    const edit2 = Edit{ .pathEdits = pe2[0..], .timestamp = 1 };
    try doc.applyEdit(edit1, alloc);
    try doc.applyEdit(edit2, alloc);

    const ab = try doc.get("a/b", alloc);
    defer alloc.free(ab);
    try std.testing.expectEqualStrings("foo", ab);
}

test "try to delete newer path" {
    const alloc = std.testing.allocator;
    var doc = try Document.init(0, alloc);
    defer doc.free(alloc);

    const pe1 = [_]PathEdit{
        .{ .PUT = .{ .path = "a", .value = "foo" } },
    };

    const pe2 = [_]PathEdit{
        .{ .DELETE = .{ .path = "a" } },
    };

    const edit1 = Edit{ .pathEdits = pe1[0..], .timestamp = 1 };
    const edit2 = Edit{ .pathEdits = pe2[0..], .timestamp = 0 };
    try doc.applyEdit(edit1, alloc);
    try doc.applyEdit(edit2, alloc);

    const a = try doc.get("a", alloc);
    defer alloc.free(a);
    try std.testing.expectEqualStrings("foo", a);
}
