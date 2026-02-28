const std = @import("std");
const Time = @import("time.zig");
const Edit = @import("edit.zig");
const PathEdit = @import("pathEdit.zig").PathEdit;
const PathEditValue = @import("pathEdit.zig").Value;
const EditAction = @import("pathEdit.zig").EditAction;
const PutStruct = @import("pathEdit.zig").PutStruct;
const pathEditPut = @import("pathEdit.zig").initPut;
const pathEditDelete = @import("pathEdit.zig").initDelete;

pub const Document = @This();

const UUID_LENGTH = 16;
// TODO: implement a replace method instead of calling conditional remove & puts

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
                if (cur.timestamp > ts) return;
                try teardownChildren(cur, ts, alloc);
                return;
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
                if (cur.timestamp > ts) return;
                try teardownChildren(cur, ts, alloc);
            }
            switch (pathEdit.value) {
                .string => try buildChildren(pathEdit.value, previous_nodes, ts, part, alloc),
                .object => {
                    if (!previous_nodes.contains(part)) {
                        const child = try Node.initChildren(ts, alloc);
                        try previous_nodes.put(part, child);
                        try buildChildren(pathEdit.value, &child.data.children, ts, part, alloc);
                    } else {
                        const cur = previous_nodes.get(part).?;
                        switch (cur.data) {
                            .children => |*children| try buildChildren(pathEdit.value, children, ts, part, alloc),
                            .value => |v| {
                                if (cur.timestamp > ts) return;
                                alloc.free(v);
                                // TODO: make this more modular like Node.initChildren(ts: i64, alloc: Allocator)
                                cur.data = .{ .children = std.StringHashMap(*Node).init(alloc) };
                                cur.timestamp = ts;
                                try buildChildren(pathEdit.value, &cur.data.children, ts, part, alloc);
                            },
                            .tombstone => {
                                if (cur.timestamp > ts) return;
                                // TODO: make this more modular like Node.initChildren(ts: i64, alloc: Allocator)
                                cur.data = .{ .children = std.StringHashMap(*Node).init(alloc) };
                                cur.timestamp = ts;
                                try buildChildren(pathEdit.value, &cur.data.children, ts, part, alloc);
                            },
                        }
                    }
                },
            }
            return;
        }

        const existing = previous_nodes.get(part);
        if (existing) |ptr| {
            switch (ptr.*.data) {
                .children => |*children| {
                    if (ptr.timestamp > ts) return;
                    previous_nodes = children;
                },
                .value => {
                    if (ptr.timestamp > ts) return;
                    freeNode(ptr, alloc);
                    const new_node = try Node.initChildren(ts, alloc);
                    try previous_nodes.put(part, new_node);
                    previous_nodes = &new_node.data.children;
                },
                .tombstone => {
                    if (ptr.timestamp > ts) return;
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

fn teardownChildren(node: *Node, ts: i64, alloc: std.mem.Allocator) !void {
    switch (node.data) {
        .children => |*children| {
            var it = children.iterator();
            while (it.next()) |entry| {
                try teardownChildren(entry.value_ptr.*, ts, alloc);
            }
            if (node.timestamp <= ts) node.timestamp = ts;
        },
        .tombstone => {
            // can compact tombstone if n.timestamp <= ts
            return;
        },
        .value => |v| {
            if (node.timestamp > ts) return;
            alloc.free(v);
            node.data = .{ .tombstone = true };
            node.timestamp = ts;
        },
    }
}

fn buildChildren(edit: PathEditValue, node: *std.StringHashMap(*Node), ts: i64, path: []const u8, alloc: std.mem.Allocator) !void {
    switch (edit) {
        .string => |str| {
            if (node.get(path)) |cur| {
                if (cur.timestamp > ts) return;
            }
            const leaf = try Node.initValue(str, ts, alloc);
            try replaceChildIfNewer(node, leaf, path, alloc);
        },
        .object => |fields| {
            for (fields) |field| {
                switch (field.value) {
                    .string => |str| {
                        if (node.get(field.key)) |cur| {
                            if (cur.timestamp > ts) continue;
                        }
                        const leaf = try Node.initValue(str, ts, alloc);
                        try replaceChildIfNewer(node, leaf, field.key, alloc);
                    },
                    .object => |_| {
                        if (node.get(field.key)) |cur| {
                            if (cur.timestamp > ts) continue;
                        }
                        const child = try Node.initChildren(ts, alloc);
                        try replaceChildIfNewer(node, child, field.key, alloc);
                        try buildChildren(field.value, &child.data.children, ts, field.key, alloc);
                    },
                }
            }
        },
    }
}

fn replaceChildIfNewer(map: *std.StringHashMap(*Node), new_node: *Node, path: []const u8, alloc: std.mem.Allocator) !void {
    if (map.fetchRemove(path)) |removed| {
        freeNode(removed.value, alloc);
    }
    try map.put(path, new_node);
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

    const p1 = [_]PathEdit{pathEditPut("a/b", .{ .string = "foo" })};
    const edit = Edit{ .pathEdits = p1[0..], .timestamp = 0 };
    try doc.applyEdit(edit, alloc);
    const abstring = try doc.get("a/b", alloc);
    defer alloc.free(abstring);
    try std.testing.expectEqualStrings("foo", abstring);
    const a = try doc.get("a", alloc);
    defer alloc.free(a);
    try std.testing.expectEqualStrings("{\"b\":\"foo\"}", a);
    try std.testing.expectEqual(0, doc.root.timestamp);
}

test "delete path from document" {
    const alloc = std.testing.allocator;
    var doc = try Document.init(0, alloc);
    defer doc.free(alloc);

    const p1 = [_]PathEdit{pathEditPut("a", .{ .string = "foobar" })};
    const p2 = [_]PathEdit{pathEditDelete("a")};
    const addEdit = Edit{ .pathEdits = p1[0..], .timestamp = 0 };
    const removeEdit = Edit{ .pathEdits = p2[0..], .timestamp = 1 };
    try doc.applyEdit(addEdit, alloc);
    try doc.applyEdit(removeEdit, alloc);
    try std.testing.expectError(error.NotFound, doc.get("a", alloc));
    try std.testing.expectEqual(1, doc.lastUpdatedTimestamp);
}

test "timestamp clash" {
    const alloc = std.testing.allocator;
    var doc = try Document.init(0, alloc);
    defer doc.free(alloc);

    const p1 = [_]PathEdit{pathEditPut("a", .{ .string = "foo" })};
    const p2 = [_]PathEdit{pathEditPut("a", .{ .string = "bar" })};
    const p3 = [_]PathEdit{pathEditDelete("a")};
    const edit1 = Edit{ .pathEdits = p1[0..], .timestamp = 2 };
    const edit2 = Edit{ .pathEdits = p2[0..], .timestamp = 1 };
    const edit3 = Edit{ .pathEdits = p3[0..], .timestamp = 0 };
    try doc.applyEdit(edit1, alloc);
    try doc.applyEdit(edit2, alloc);
    try doc.applyEdit(edit3, alloc);

    const a = try doc.get("a", alloc);
    defer alloc.free(a);
    try std.testing.expectEqualStrings("foo", a);
}

test "tombstone: newer delete first" {
    const alloc = std.testing.allocator;
    var doc = try Document.init(0, alloc);
    defer doc.free(alloc);

    const p1 = [_]PathEdit{pathEditDelete("a")};
    const p2 = [_]PathEdit{pathEditPut("a", .{ .string = "foo" })};

    const edit1 = Edit{ .pathEdits = p1[0..], .timestamp = 1 };
    const edit2 = Edit{ .pathEdits = p2[0..], .timestamp = 0 };
    try doc.applyEdit(edit1, alloc);
    try doc.applyEdit(edit2, alloc);

    try std.testing.expectError(error.NotFound, doc.get("a", alloc));
}

test "tombstone: ancestor deny" {
    const alloc = std.testing.allocator;
    var doc = try Document.init(0, alloc);
    defer doc.free(alloc);

    const p1 = [_]PathEdit{pathEditDelete("a")};
    const p2 = [_]PathEdit{pathEditPut("a/b", .{ .string = "foo" })};

    const edit1 = Edit{ .pathEdits = p1[0..], .timestamp = 1 };
    const edit2 = Edit{ .pathEdits = p2[0..], .timestamp = 0 };
    try doc.applyEdit(edit1, alloc);
    try doc.applyEdit(edit2, alloc);

    try std.testing.expectError(error.NotFound, doc.get("a/b", alloc));
}

test "tombstone: ancestor allow" {
    const alloc = std.testing.allocator;
    var doc = try Document.init(0, alloc);
    defer doc.free(alloc);

    const p1 = [_]PathEdit{pathEditDelete("a")};
    const p2 = [_]PathEdit{pathEditPut("a/b", .{ .string = "foo" })};

    const edit1 = Edit{ .pathEdits = p1[0..], .timestamp = 0 };
    const edit2 = Edit{ .pathEdits = p2[0..], .timestamp = 1 };
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

    const p1 = [_]PathEdit{pathEditPut("a", .{ .string = "foo" })};
    const p2 = [_]PathEdit{pathEditDelete("b")};

    const edit1 = Edit{ .pathEdits = p1[0..], .timestamp = 1 };
    const edit2 = Edit{ .pathEdits = p2[0..], .timestamp = 0 };
    try doc.applyEdit(edit1, alloc);
    try doc.applyEdit(edit2, alloc);

    const a = try doc.get("a", alloc);
    defer alloc.free(a);
    try std.testing.expectEqualStrings("foo", a);
}

test "nested edit" {
    const alloc = std.testing.allocator;
    var doc = try Document.init(0, alloc);
    defer doc.free(alloc);

    const p1 = [_]PathEdit{pathEditPut("a", .{
        .object = &[_]PathEditValue.Field{
            .{ .key = "b", .value = .{ .string = "1" } },
            .{ .key = "c", .value = .{ .string = "2" } },
        },
    })};
    const p2 = [_]PathEdit{pathEditPut("a/b", .{ .string = "3" })};

    const edit1 = Edit{ .pathEdits = p1[0..], .timestamp = 1 };
    const edit2 = Edit{ .pathEdits = p2[0..], .timestamp = 0 };
    try doc.applyEdit(edit1, alloc);
    try doc.applyEdit(edit2, alloc);

    const ab = try doc.get("a/b", alloc);
    defer alloc.free(ab);
    const ac = try doc.get("a/c", alloc);
    defer alloc.free(ac);
    try std.testing.expectEqualStrings("1", ab);
    try std.testing.expectEqualStrings("2", ac);
}

test "newer ancestor nested edit" {
    const alloc = std.testing.allocator;
    var doc = try Document.init(0, alloc);
    defer doc.free(alloc);
    const p1 = [_]PathEdit{pathEditPut("a", .{
        .object = &[_]PathEditValue.Field{
            .{ .key = "b", .value = .{ .string = "new" } },
        },
    })};
    const p2 = [_]PathEdit{pathEditPut("a/c", .{ .string = "old" })};
    const edit1 = Edit{ .pathEdits = p1[0..], .timestamp = 1 };
    const edit2 = Edit{ .pathEdits = p2[0..], .timestamp = 0 };
    try doc.applyEdit(edit1, alloc);
    try doc.applyEdit(edit2, alloc);

    const ab = try doc.get("a/b", alloc);
    defer alloc.free(ab);
    try std.testing.expectEqualStrings("new", ab);
    try std.testing.expectError(error.NotFound, doc.get("a/c", alloc));
}

test "string -> object allow" {
    const alloc = std.testing.allocator;
    var doc = try Document.init(0, alloc);
    defer doc.free(alloc);

    const p1 = [_]PathEdit{pathEditPut("a", .{ .string = "old" })};
    const p2 = [_]PathEdit{pathEditPut("a", .{
        .object = &[_]PathEditValue.Field{
            .{ .key = "b", .value = .{ .string = "new" } },
        },
    })};

    try doc.applyEdit(Edit{ .pathEdits = p1[0..], .timestamp = 1 }, alloc);
    try doc.applyEdit(Edit{ .pathEdits = p2[0..], .timestamp = 2 }, alloc);

    const ab = try doc.get("a/b", alloc);
    defer alloc.free(ab);
    try std.testing.expectEqualStrings("new", ab);
}

test "string -> object deny" {
    const alloc = std.testing.allocator;
    var doc = try Document.init(0, alloc);
    defer doc.free(alloc);

    const p1 = [_]PathEdit{pathEditPut("a", .{ .string = "newer" })};
    const p2 = [_]PathEdit{pathEditPut("a", .{
        .object = &[_]PathEditValue.Field{
            .{ .key = "b", .value = .{ .string = "older" } },
        },
    })};

    try doc.applyEdit(Edit{ .pathEdits = p1[0..], .timestamp = 2 }, alloc);
    try doc.applyEdit(Edit{ .pathEdits = p2[0..], .timestamp = 1 }, alloc);

    const a = try doc.get("a", alloc);
    defer alloc.free(a);
    try std.testing.expectEqualStrings("newer", a);
    try std.testing.expectError(error.NotFound, doc.get("a/b", alloc));
}

test "tombstone -> object allow" {
    const alloc = std.testing.allocator;
    var doc = try Document.init(0, alloc);
    defer doc.free(alloc);

    const del = [_]PathEdit{pathEditDelete("a")};
    const put = [_]PathEdit{pathEditPut("a", .{
        .object = &[_]PathEditValue.Field{
            .{ .key = "b", .value = .{ .string = "alive" } },
        },
    })};

    try doc.applyEdit(Edit{ .pathEdits = del[0..], .timestamp = 1 }, alloc);
    try doc.applyEdit(Edit{ .pathEdits = put[0..], .timestamp = 2 }, alloc);

    const ab = try doc.get("a/b", alloc);
    defer alloc.free(ab);
    try std.testing.expectEqualStrings("alive", ab);
}

test "tombstone -> object deny" {
    const alloc = std.testing.allocator;
    var doc = try Document.init(0, alloc);
    defer doc.free(alloc);

    const del = [_]PathEdit{pathEditDelete("a")};
    const put = [_]PathEdit{pathEditPut("a", .{
        .object = &[_]PathEditValue.Field{
            .{ .key = "b", .value = .{ .string = "old" } },
        },
    })};

    try doc.applyEdit(Edit{ .pathEdits = del[0..], .timestamp = 2 }, alloc);
    try doc.applyEdit(Edit{ .pathEdits = put[0..], .timestamp = 1 }, alloc);

    try std.testing.expectError(error.NotFound, doc.get("a", alloc));
    try std.testing.expectError(error.NotFound, doc.get("a/b", alloc));
}

test "partial ancestor edit" {
    const alloc = std.testing.allocator;
    var doc = try Document.init(0, alloc);
    defer doc.free(alloc);

    const p1 = [_]PathEdit{pathEditPut("a", .{
        .object = &[_]PathEditValue.Field{
            .{ .key = "b", .value = .{ .string = "0" } },
            .{ .key = "c", .value = .{ .string = "0" } },
        },
    })};
    const p2 = [_]PathEdit{pathEditPut("a/d", .{ .string = "10" })};
    const p3 = [_]PathEdit{pathEditPut("a", .{
        .object = &[_]PathEditValue.Field{
            .{ .key = "b", .value = .{ .string = "1" } },
            .{ .key = "c", .value = .{ .string = "1" } },
        },
    })};

    const edit1 = Edit{ .pathEdits = p1[0..], .timestamp = 1 };
    const edit2 = Edit{ .pathEdits = p2[0..], .timestamp = 3 };
    const edit3 = Edit{ .pathEdits = p3[0..], .timestamp = 2 };
    try doc.applyEdit(edit1, alloc);
    try doc.applyEdit(edit2, alloc);
    try doc.applyEdit(edit3, alloc);

    const ab = try doc.get("a/b", alloc);
    defer alloc.free(ab);
    try std.testing.expectEqualStrings("1", ab);
    const ac = try doc.get("a/c", alloc);
    defer alloc.free(ac);
    try std.testing.expectEqualStrings("1", ac);
    const ad = try doc.get("a/d", alloc);
    defer alloc.free(ad);
    try std.testing.expectEqualStrings("10", ad);
}
