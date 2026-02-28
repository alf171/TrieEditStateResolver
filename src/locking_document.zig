const std = @import("std");
const Edit = @import("edit.zig");
const Document = @import("document.zig");

pub const LockingDocument = @This();

doc: Document,
mutex: std.Thread.Mutex,

pub fn init(ts: i64, alloc: std.mem.Allocator) !LockingDocument {
    return .{
        .doc = try Document.init(ts, alloc),
        .mutex = .{},
    };
}

pub fn free(self: *LockingDocument, alloc: std.mem.Allocator) void {
    self.mutex.lock();
    defer self.mutex.unlock();
    self.doc.free(alloc);
}

pub fn applyEdit(self: *LockingDocument, edit: Edit, alloc: std.mem.Allocator) !void {
    self.mutex.lock();
    defer self.mutex.unlock();
    try self.doc.applyEdit(edit, alloc);
}

pub fn get(self: *LockingDocument, path: []const u8, alloc: std.mem.Allocator) error{ NotFound, NotImplemented, OutOfMemory, WriteFailed }![]const u8 {
    self.mutex.lock();
    defer self.mutex.unlock();
    return try self.doc.get(path, alloc);
}

pub fn print(self: *LockingDocument, writer: *std.io.Writer, alloc: std.mem.Allocator) !void {
    return try self.doc.print(writer, alloc);
}

test "init" {
    const alloc = std.testing.allocator;
    var doc = try LockingDocument.init(0, alloc);
    defer doc.free(alloc);
}
