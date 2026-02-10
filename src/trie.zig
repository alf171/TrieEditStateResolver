const std = @import("std");

pub const TrieNode = @This();

key: []const u8,
value: []const u8,
decendents: []const TrieNode,
