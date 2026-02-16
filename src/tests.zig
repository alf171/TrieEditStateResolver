const std = @import("std");

test {
    std.testing.refAllDecls(@import("loadtest/batch_queue.zig"));
}
