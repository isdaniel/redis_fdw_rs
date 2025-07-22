/// Redis connection management types and utilities
/// This module handles different types of Redis connections and their management

use redis::cluster::ClusterConnection;

/// Enum representing different Redis connection types
pub enum RedisConnectionType {
    Single(redis::Connection),
    Cluster(ClusterConnection),
}

impl RedisConnectionType {
    pub fn as_connection_like_mut(&mut self) -> &mut dyn redis::ConnectionLike {
        match self {
            RedisConnectionType::Single(conn) => conn,
            RedisConnectionType::Cluster(conn) => conn,
        }
    }
}
