//! SQLite-backed lease election with monotonically increasing fencing terms.

#![allow(clippy::result_large_err)]

use chrono::{DateTime, Utc};
use sqlx::Row;
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::{
    db::Database,
    proto::{
        election_service_server::ElectionService as ElectionServiceTrait, ElectionCampaignRequest,
        ElectionCampaignResponse, ElectionGetLeaderRequest, ElectionGetLeaderResponse,
        ElectionLeader, ElectionRenewRequest, ElectionRenewResponse, ElectionResignRequest,
        ElectionResignResponse,
    },
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaderLease {
    pub election: String,
    pub leader_id: String,
    pub term: u64,
    pub lease_id: String,
    pub expire_at_ms: i64,
}

impl LeaderLease {
    pub fn active(&self) -> bool {
        self.expire_at_ms > now_millis()
    }

    fn to_proto(&self) -> ElectionLeader {
        ElectionLeader {
            election: self.election.clone(),
            leader_id: self.leader_id.clone(),
            term: self.term,
            lease_id: self.lease_id.clone(),
            expire_at: timestamp(self.expire_at_ms),
            remaining_ttl_ms: self.expire_at_ms.saturating_sub(now_millis()).max(0),
        }
    }
}

#[derive(Clone)]
pub struct ElectionStore {
    db: Database,
}

impl ElectionStore {
    pub fn new(db: Database) -> Self {
        Self { db }
    }

    /// Campaign atomically. An expired election advances the fencing term;
    /// an active election is returned unchanged so every candidate observes
    /// the same leader.
    pub async fn campaign(
        &self,
        election: &str,
        candidate_id: &str,
        ttl_seconds: i64,
    ) -> anyhow::Result<(bool, LeaderLease)> {
        let now = now_millis();
        let expires = now.saturating_add(ttl_seconds.saturating_mul(1000));
        let lease_id = Uuid::new_v4().to_string();
        let row = sqlx::query(
            "INSERT INTO sys_election(election_key,leader_id,term,lease_id,expire_at_ms,update_time)
             VALUES(?,?,1,?,?,?)
             ON CONFLICT(election_key) DO UPDATE SET
                leader_id=excluded.leader_id,
                term=sys_election.term+1,
                lease_id=excluded.lease_id,
                expire_at_ms=excluded.expire_at_ms,
                update_time=excluded.update_time
             WHERE sys_election.expire_at_ms<=?
             RETURNING election_key,leader_id,term,lease_id,expire_at_ms",
        )
        .bind(election)
        .bind(candidate_id)
        .bind(lease_id)
        .bind(expires)
        .bind(timestamp(now))
        .bind(now)
        .fetch_optional(&self.db.0)
        .await?;
        let leader = match row {
            Some(row) => hydrate(row),
            None => self
                .get(election)
                .await?
                .ok_or_else(|| anyhow::anyhow!("election disappeared during campaign"))?,
        };
        Ok((leader.active() && leader.leader_id == candidate_id, leader))
    }

    /// Renew only a currently active, exact leader/term/lease tuple. An
    /// expired lease can never be revived.
    pub async fn renew(
        &self,
        election: &str,
        leader_id: &str,
        term: u64,
        lease_id: &str,
        ttl_seconds: i64,
    ) -> anyhow::Result<Option<LeaderLease>> {
        let term = i64::try_from(term)?;
        let now = now_millis();
        let expires = now.saturating_add(ttl_seconds.saturating_mul(1000));
        let row = sqlx::query(
            "UPDATE sys_election SET expire_at_ms=?,update_time=?
             WHERE election_key=? AND leader_id=? AND term=? AND lease_id=? AND expire_at_ms>?
             RETURNING election_key,leader_id,term,lease_id,expire_at_ms",
        )
        .bind(expires)
        .bind(timestamp(now))
        .bind(election)
        .bind(leader_id)
        .bind(term)
        .bind(lease_id)
        .bind(now)
        .fetch_optional(&self.db.0)
        .await?;
        Ok(row.map(hydrate))
    }

    pub async fn resign(
        &self,
        election: &str,
        leader_id: &str,
        term: u64,
        lease_id: &str,
    ) -> anyhow::Result<bool> {
        let term = i64::try_from(term)?;
        let now = now_millis();
        Ok(sqlx::query(
            "UPDATE sys_election SET expire_at_ms=?,update_time=?
             WHERE election_key=? AND leader_id=? AND term=? AND lease_id=? AND expire_at_ms>?",
        )
        .bind(now)
        .bind(timestamp(now))
        .bind(election)
        .bind(leader_id)
        .bind(term)
        .bind(lease_id)
        .bind(now)
        .execute(&self.db.0)
        .await?
        .rows_affected()
            == 1)
    }

    pub async fn get(&self, election: &str) -> anyhow::Result<Option<LeaderLease>> {
        Ok(sqlx::query(
            "SELECT election_key,leader_id,term,lease_id,expire_at_ms FROM sys_election WHERE election_key=?",
        )
        .bind(election)
        .fetch_optional(&self.db.0)
        .await?
        .map(hydrate))
    }
}

#[derive(Clone)]
pub struct ElectionService {
    store: ElectionStore,
}

impl ElectionService {
    pub fn new(store: ElectionStore) -> Self {
        Self { store }
    }
}

#[tonic::async_trait]
impl ElectionServiceTrait for ElectionService {
    async fn campaign(
        &self,
        request: Request<ElectionCampaignRequest>,
    ) -> Result<Response<ElectionCampaignResponse>, Status> {
        let request = request.into_inner();
        let election = required(&request.election, "election")?;
        let candidate = required(&request.candidate_id, "candidate_id")?;
        validate_ttl(request.ttl_seconds)?;
        let (won, leader) = self
            .store
            .campaign(election, candidate, request.ttl_seconds)
            .await
            .map_err(internal("campaign"))?;
        Ok(Response::new(ElectionCampaignResponse {
            won,
            leader: Some(leader.to_proto()),
        }))
    }

    async fn renew(
        &self,
        request: Request<ElectionRenewRequest>,
    ) -> Result<Response<ElectionRenewResponse>, Status> {
        let request = request.into_inner();
        let election = required(&request.election, "election")?;
        let leader_id = required(&request.leader_id, "leader_id")?;
        let lease_id = required(&request.lease_id, "lease_id")?;
        if request.term == 0 {
            return Err(Status::invalid_argument("term must be > 0"));
        }
        validate_ttl(request.ttl_seconds)?;
        let leader = self
            .store
            .renew(
                election,
                leader_id,
                request.term,
                lease_id,
                request.ttl_seconds,
            )
            .await
            .map_err(internal("renew"))?;
        Ok(Response::new(ElectionRenewResponse {
            renewed: leader.is_some(),
            leader: leader.map(|leader| leader.to_proto()),
        }))
    }

    async fn resign(
        &self,
        request: Request<ElectionResignRequest>,
    ) -> Result<Response<ElectionResignResponse>, Status> {
        let request = request.into_inner();
        let election = required(&request.election, "election")?;
        let leader_id = required(&request.leader_id, "leader_id")?;
        let lease_id = required(&request.lease_id, "lease_id")?;
        if request.term == 0 {
            return Err(Status::invalid_argument("term must be > 0"));
        }
        let resigned = self
            .store
            .resign(election, leader_id, request.term, lease_id)
            .await
            .map_err(internal("resign"))?;
        Ok(Response::new(ElectionResignResponse { resigned }))
    }

    async fn get_leader(
        &self,
        request: Request<ElectionGetLeaderRequest>,
    ) -> Result<Response<ElectionGetLeaderResponse>, Status> {
        let request = request.into_inner();
        let election = required(&request.election, "election")?;
        let leader = self.store.get(election).await.map_err(internal("get"))?;
        Ok(Response::new(ElectionGetLeaderResponse {
            found: leader.is_some(),
            active: leader.as_ref().is_some_and(LeaderLease::active),
            leader: leader.map(|leader| leader.to_proto()),
        }))
    }
}

fn hydrate(row: sqlx::sqlite::SqliteRow) -> LeaderLease {
    LeaderLease {
        election: row.get("election_key"),
        leader_id: row.get("leader_id"),
        term: row.get::<i64, _>("term") as u64,
        lease_id: row.get("lease_id"),
        expire_at_ms: row.get("expire_at_ms"),
    }
}

fn required<'a>(value: &'a str, name: &str) -> Result<&'a str, Status> {
    let value = value.trim();
    if value.is_empty() {
        Err(Status::invalid_argument(format!("{name} is required")))
    } else {
        Ok(value)
    }
}

fn validate_ttl(ttl: i64) -> Result<(), Status> {
    if !(1..=3600).contains(&ttl) {
        Err(Status::invalid_argument(
            "ttl_seconds must be between 1 and 3600",
        ))
    } else {
        Ok(())
    }
}

fn internal(operation: &'static str) -> impl FnOnce(anyhow::Error) -> Status {
    move |error| {
        tracing::error!(%error, operation, "election operation failed");
        Status::internal(format!("election {operation} failed"))
    }
}

fn now_millis() -> i64 {
    Utc::now().timestamp_millis()
}

fn timestamp(millis: i64) -> String {
    DateTime::from_timestamp_millis(millis)
        .unwrap_or(DateTime::UNIX_EPOCH)
        .to_rfc3339()
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn store() -> ElectionStore {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        ElectionStore::new(db)
    }

    #[tokio::test]
    async fn election_uses_monotonic_fencing_terms() {
        let store = store().await;
        let (won, first) = store.campaign("map/master", "map-0", 15).await.unwrap();
        assert!(won);
        assert_eq!(first.term, 1);
        let (won, observed) = store.campaign("map/master", "map-1", 15).await.unwrap();
        assert!(!won);
        assert_eq!(observed, first);
        assert!(store
            .resign("map/master", &first.leader_id, first.term, &first.lease_id)
            .await
            .unwrap());
        let (won, second) = store.campaign("map/master", "map-1", 15).await.unwrap();
        assert!(won);
        assert_eq!(second.term, 2);
    }

    #[tokio::test]
    async fn expired_lease_cannot_be_renewed() {
        let store = store().await;
        let (_, leader) = store.campaign("map/master", "map-0", 15).await.unwrap();
        sqlx::query("UPDATE sys_election SET expire_at_ms=0 WHERE election_key='map/master'")
            .execute(&store.db.0)
            .await
            .unwrap();
        assert!(store
            .renew(
                "map/master",
                &leader.leader_id,
                leader.term,
                &leader.lease_id,
                15
            )
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn elections_are_isolated_by_application_defined_name() {
        let store = store().await;
        let (_, map) = store
            .campaign("map/render-primary", "map-0", 15)
            .await
            .unwrap();
        let (_, update) = store
            .campaign("update/reconciler-primary", "update-0", 15)
            .await
            .unwrap();
        assert_eq!(map.term, 1);
        assert_eq!(update.term, 1);
        assert_eq!(store.get("map/render-primary").await.unwrap(), Some(map));
        assert_eq!(
            store.get("update/reconciler-primary").await.unwrap(),
            Some(update)
        );
    }

    #[tokio::test]
    async fn stale_fencing_term_cannot_renew_new_leader() {
        let store = store().await;
        let (_, first) = store
            .campaign("jobs/dispatcher", "jobs-0", 15)
            .await
            .unwrap();
        assert!(store
            .resign(
                &first.election,
                &first.leader_id,
                first.term,
                &first.lease_id
            )
            .await
            .unwrap());
        let (_, second) = store
            .campaign("jobs/dispatcher", "jobs-1", 15)
            .await
            .unwrap();
        assert!(second.term > first.term);
        assert!(store
            .renew(
                &first.election,
                &first.leader_id,
                first.term,
                &first.lease_id,
                15
            )
            .await
            .unwrap()
            .is_none());
    }
}
