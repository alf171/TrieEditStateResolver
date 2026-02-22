const std = @import("std");
const Edit = @import("edit.zig");
const PathEdit = @import("pathEdit.zig").PathEdit;
const EditAction = @import("pathEdit.zig").EditAction;
const PutStruct = @import("pathEdit.zig").PutStruct;

pub const Document = @This();

const UUID_LENGTH = 16;

const Node = union(enum) {
    nodes: std.StringHashMap(*Node),
    value: []const u8,

    pub fn initNodes(alloc: std.mem.Allocator) !*Node {
        const node = try alloc.create(Node);
        node.* = .{ .nodes = std.StringHashMap(*Node).init(alloc) };
        return node;
    }

    pub fn initLeaf(value: []const u8, alloc: std.mem.Allocator) !*Node {
        const node = try alloc.create(Node);
        node.* = .{ .value = try alloc.dupe(u8, value) };
        return node;
    }
};

root: *Node,
/// document will own these for nows not json blob
uuid: []u8,
editCount: u64,

pub fn init(alloc: std.mem.Allocator) !Document {
    const uuid = try alloc.alloc(u8, UUID_LENGTH);
    const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789";
    for (uuid) |*ch| {
        const idx = std.crypto.random.intRangeAtMost(u8, 0, alphabet.len - 1);
        ch.* = alphabet[idx];
    }
    return .{ .root = try Node.initNodes(alloc), .uuid = uuid, .editCount = 0 };
}

pub fn free(self: Document, alloc: std.mem.Allocator) void {
    alloc.free(self.uuid);
    freeNode(self.root, alloc);
}

fn freeNode(node: *Node, alloc: std.mem.Allocator) void {
    switch (node.*) {
        .value => |v| {
            alloc.free(v);
        },
        .nodes => |*map| {
            var it = map.iterator();
            while (it.next()) |cur| {
                const child = cur.value_ptr.*;
                freeNode(child, alloc);
            }
            map.deinit();
        },
    }
    alloc.destroy(node);
}

pub fn print(self: Document, writer: *std.io.Writer) !void {
    try writer.print("{{\"id\":\"{s}\",", .{self.uuid});
    try writer.print("\"editCount\":\"{d}\",\"data\":", .{self.editCount});
    try printNode(self.root, writer);
    try writer.print("}}", .{});
}

fn printNode(node: *const Node, writer: *std.io.Writer) !void {
    switch (node.*) {
        .value => |v| {
            try writer.print("\"{s}\"", .{v});
        },
        .nodes => |map| {
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
    }
}

/// apply an edit onto a document
/// does not consider timestamp atm
pub fn applyEdit(self: *Document, edit: Edit, alloc: std.mem.Allocator) !void {
    self.editCount += 1;
    for (edit.pathEdits) |pathEdit| {
        try switch (pathEdit) {
            EditAction.PUT => |put| applyEditPut(self, put, alloc),
            EditAction.DELETE => |del| applyEditDelete(self, del.path, alloc),
        };
    }
}

fn applyEditDelete(self: *Document, path: []const u8, alloc: std.mem.Allocator) !void {
    var parts = std.mem.splitSequence(u8, path, "/");
    var previous_nodes = &self.root.nodes;
    var next_part = parts.next() orelse return;
    while (true) {
        const part = next_part;
        const next = parts.next();
        const is_last = (next == null);

        if (is_last) {
            if (previous_nodes.fetchRemove(part)) |removed| {
                freeNode(removed.value, alloc);
            }
            return;
        }

        const existing = previous_nodes.get(part) orelse return;
        switch (existing.*) {
            .nodes => |*current_nodes| previous_nodes = current_nodes,
            .value => {
                return;
            },
        }

        next_part = next.?;
    }
}

fn applyEditPut(self: *Document, pathEdit: PutStruct, alloc: std.mem.Allocator) !void {
    var parts = std.mem.splitSequence(u8, pathEdit.path, "/");
    var previous_nodes = &self.root.nodes;
    var next_part = parts.next() orelse return;
    while (true) {
        const part = next_part;
        const next = parts.next();
        const is_last = (next == null);

        if (is_last) {
            if (previous_nodes.fetchRemove(part)) |removed| {
                freeNode(removed.value, alloc);
            }
            const leaf = try Node.initLeaf(pathEdit.value, alloc);
            try previous_nodes.put(part, leaf);
            return;
        }

        const existing = previous_nodes.get(part);
        if (existing) |ptr| {
            switch (ptr.*) {
                .nodes => |*current_nodes| previous_nodes = current_nodes,
                .value => {
                    freeNode(ptr, alloc);
                    const new_node = try Node.initNodes(alloc);
                    try previous_nodes.put(part, new_node);
                    previous_nodes = &new_node.nodes;
                },
            }
        } else {
            const child = try Node.initNodes(alloc);
            try previous_nodes.put(part, child);
            previous_nodes = &child.nodes;
        }
        next_part = next.?;
    }
}

pub fn get(self: *Document, path: []const u8) error{ NotFound, NotImplemented }![]const u8 {
    var parts = std.mem.splitSequence(u8, path, "/");
    var curBlob: *const Node = self.root;
    while (parts.next()) |part| {
        switch (curBlob.*) {
            .nodes => |map| {
                curBlob = map.get(part) orelse return error.NotFound;
            },
            .value => {},
        }
    }
    return switch (curBlob.*) {
        .nodes => error.NotImplemented,
        .value => |value| value,
    };
}

test "put edit on document" {
    const alloc = std.testing.allocator;
    var doc = try Document.init(alloc);
    defer doc.free(alloc);

    const pathEdits = [_]PathEdit{.{ .PUT = .{
        .path = "a",
        .value = "foo",
    } }};
    const edit = Edit{ .pathEdits = pathEdits[0..], .timestamp = 0 };
    try doc.applyEdit(edit, alloc);
    try std.testing.expectEqualSlices(u8, "foo", try doc.get("a"));
}

test "delete path from document" {
    const alloc = std.testing.allocator;
    var doc = try Document.init(alloc);
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
    try std.testing.expectEqual(error.NotFound, doc.get("a"));
}
