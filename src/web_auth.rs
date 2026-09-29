use anyhow::{Context, Result};
use argon2::{
    password_hash::{rand_core::OsRng, PasswordHash, PasswordHasher, PasswordVerifier, SaltString},
    Argon2,
};
use sqlx::{sqlite::SqlitePoolOptions, SqlitePool};
use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::Semaphore;

const DEFAULT_ADMIN_PASSWORD: &str = "-Cangling@zky";
const SESSION_SECONDS: u64 = 2 * 60 * 60;

#[derive(Clone)]
pub struct WebAuth {
    database: SqlitePool,
    login_slots: Arc<Semaphore>,
}

impl WebAuth {
    pub async fn open(url: &str) -> Result<Self> {
        let database = SqlitePoolOptions::new()
            .max_connections(2)
            .connect(url)
            .await?;
        sqlx::query("CREATE TABLE IF NOT EXISTS admin_user(username TEXT PRIMARY KEY,password_hash TEXT NOT NULL)")
            .execute(&database).await?;
        sqlx::query("CREATE TABLE IF NOT EXISTS admin_sessions(token TEXT PRIMARY KEY,last_seen INTEGER NOT NULL)")
            .execute(&database).await?;
        Ok(Self {
            database,
            login_slots: Arc::new(Semaphore::new(4)),
        })
    }

    pub async fn ensure_initial_admin(&self, configured: Option<&str>) -> Result<()> {
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM admin_user")
            .fetch_one(&self.database)
            .await?;
        if count > 0 {
            return Ok(());
        }
        let password = configured
            .filter(|value| !value.trim().is_empty())
            .unwrap_or(DEFAULT_ADMIN_PASSWORD);
        self.set_password(password).await?;
        if configured.is_none() {
            tracing::warn!("Dashboard 管理员使用缺省初始密码创建，请登录后立即修改");
        }
        Ok(())
    }

    pub async fn reset_password(&self, password: Option<String>) -> Result<()> {
        let policy =
            crate::password_policy::PasswordPolicy::from_env().map_err(anyhow::Error::msg)?;
        let generated = password.is_none();
        let password = match password {
            Some(value) => value,
            None => policy.generate().map_err(anyhow::Error::msg)?,
        };
        self.set_password(&password).await?;
        eprintln!("已重置用户 admin 的密码，旧登录会话已全部失效。");
        if generated {
            println!("{password}");
            eprintln!("请立即登录。以上密码只显示一次。");
        }
        Ok(())
    }

    async fn set_password(&self, password: &str) -> Result<()> {
        crate::password_policy::PasswordPolicy::from_env()
            .map_err(anyhow::Error::msg)?
            .validate(password)
            .map_err(anyhow::Error::msg)?;
        let value = password.to_owned();
        let hash = tokio::task::spawn_blocking(move || hash(&value)).await??;
        sqlx::query("INSERT INTO admin_user(username,password_hash) VALUES('admin',?) ON CONFLICT(username) DO UPDATE SET password_hash=excluded.password_hash")
            .bind(hash).execute(&self.database).await.context("保存管理员密码")?;
        sqlx::query("DELETE FROM admin_sessions")
            .execute(&self.database)
            .await?;
        Ok(())
    }

    pub async fn login(&self, password: String) -> Result<Option<String>> {
        let _permit = self
            .login_slots
            .acquire()
            .await
            .map_err(|_| anyhow::anyhow!("login limiter stopped"))?;
        let hash = sqlx::query_scalar::<_, String>(
            "SELECT password_hash FROM admin_user WHERE username='admin'",
        )
        .fetch_optional(&self.database)
        .await?;
        let Some(hash) = hash else { return Ok(None) };
        let valid = tokio::task::spawn_blocking(move || verify(&password, &hash)).await?;
        if !valid {
            tokio::time::sleep(Duration::from_millis(500)).await;
            return Ok(None);
        }
        let token = uuid::Uuid::new_v4().to_string();
        sqlx::query("INSERT INTO admin_sessions(token,last_seen) VALUES(?,?)")
            .bind(&token)
            .bind(now() as i64)
            .execute(&self.database)
            .await?;
        Ok(Some(token))
    }

    pub async fn valid_session(&self, token: &str) -> Result<bool> {
        let seen =
            sqlx::query_scalar::<_, i64>("SELECT last_seen FROM admin_sessions WHERE token=?")
                .bind(token)
                .fetch_optional(&self.database)
                .await?;
        let Some(seen) = seen else { return Ok(false) };
        if now().saturating_sub(seen.max(0) as u64) > SESSION_SECONDS {
            sqlx::query("DELETE FROM admin_sessions WHERE token=?")
                .bind(token)
                .execute(&self.database)
                .await?;
            return Ok(false);
        }
        sqlx::query("UPDATE admin_sessions SET last_seen=? WHERE token=?")
            .bind(now() as i64)
            .bind(token)
            .execute(&self.database)
            .await?;
        Ok(true)
    }

    pub async fn logout(&self, token: &str) -> Result<()> {
        sqlx::query("DELETE FROM admin_sessions WHERE token=?")
            .bind(token)
            .execute(&self.database)
            .await?;
        Ok(())
    }
}

pub fn cookie_token(headers: &axum::http::HeaderMap) -> Option<&str> {
    headers
        .get(axum::http::header::COOKIE)?
        .to_str()
        .ok()?
        .split(';')
        .map(str::trim)
        .find_map(|part| part.strip_prefix("cangling_broker_session="))
}

fn hash(password: &str) -> Result<String> {
    Ok(Argon2::default()
        .hash_password(password.as_bytes(), &SaltString::generate(&mut OsRng))?
        .to_string())
}
fn verify(password: &str, hash: &str) -> bool {
    PasswordHash::new(hash).ok().is_some_and(|parsed| {
        Argon2::default()
            .verify_password(password.as_bytes(), &parsed)
            .is_ok()
    })
}
fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn initial_password_login_and_logout_work() {
        let directory = tempfile::tempdir().unwrap();
        let url = format!("sqlite://{}", directory.path().join("queue.db").display());
        let database = crate::db::Database::connect(&url).await.unwrap();
        let auth = WebAuth::open(&url).await.unwrap();
        auth.ensure_initial_admin(None).await.unwrap();

        assert!(auth.login("wrong".to_owned()).await.unwrap().is_none());
        let token = auth
            .login(DEFAULT_ADMIN_PASSWORD.to_owned())
            .await
            .unwrap()
            .unwrap();
        assert!(auth.valid_session(&token).await.unwrap());
        auth.logout(&token).await.unwrap();
        assert!(!auth.valid_session(&token).await.unwrap());
        database.close().await;
    }
}
