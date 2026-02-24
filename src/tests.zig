const std = @import("std");

test {
    std.testing.refAllDecls(@import("queues/ring_buffer.zig"));
    std.testing.refAllDecls(@import("queues/non_blocking_queue.zig"));
    std.testing.refAllDecls(@import("queues/concurrent_queue.zig"));
    std.testing.refAllDecls(@import("database.zig"));
    std.testing.refAllDecls(@import("loadtest/event_loop.zig"));
    std.testing.refAllDecls(@import("document.zig"));
    std.testing.refAllDecls(@import("pathEdit.zig"));
    std.testing.refAllDecls(@import("generator.zig"));
}
