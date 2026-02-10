const std = @import("std");
const Edit = @import("edit.zig");

pub const Document = @This();

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
        node.* = .{ .value = value };
        return node;
    }
};

root: *Node,

pub fn init(alloc: std.mem.Allocator) !Document {
    return .{ .root = try Node.initNodes(alloc) };
}

pub fn free(self: Document, alloc: std.mem.Allocator) void {
    freeNode(self.root, alloc);
}

fn freeNode(node: *Node, alloc: std.mem.Allocator) void {
    switch (node.*) {
        .value => {},
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
    try printNode(self.root, writer);
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
pub fn applyEdit(self: *Document, edit: Edit, alloc: std.mem.Allocator) !void {
    for (edit.pathEdits) |pathEdit| {
        var parts = std.mem.splitSequence(u8, pathEdit.path, "/");
        var previous_nodes = &self.root.nodes;

        var next_part = parts.next() orelse continue;
        while (true) {
            const part = next_part;
            const next = parts.next();
            const is_last = (next == null);

            if (is_last) {
                const leaf = try Node.initLeaf(pathEdit.value, alloc);
                try previous_nodes.put(part, leaf);
                break;
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
}
