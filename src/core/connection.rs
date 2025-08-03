/// Redis connection management types and utilities
/// This module handles different types of Redis connections and their management
use redis::{cluster::{ClusterClient}, Client};

/// Enum representing different Redis connection types
pub enum RedisConnectionType {
    Single(r2d2::PooledConnection<Client>),
    Cluster(r2d2::PooledConnection<ClusterClient>),
}

impl RedisConnectionType {
    pub fn as_connection_like_mut(&mut self) -> &mut dyn redis::ConnectionLike {
        match self {
            RedisConnectionType::Single(conn) => conn,
            RedisConnectionType::Cluster(conn) => conn,
        }
    }
}
