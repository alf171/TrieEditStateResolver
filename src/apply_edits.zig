const std = @import("std");

const Document = @import("document.zig");
const Edit = @import("edit.zig");

const Options = enum { SEQUENTIAL, CONCURRENT };

/// aplies edits depending on options provided
pub fn apply(document: *Document, edits: []const Edit, alloc: std.mem.Allocator, options: Options) !void {
    switch (options) {
        .SEQUENTIAL => {
            for (edits) |edit| {
                try document.applyEdit(edit, alloc);
                edit.free(alloc);
            }
            alloc.free(edits);
        },
        .CONCURRENT => {
            return error.NotImplemented;
        },
    }
}
