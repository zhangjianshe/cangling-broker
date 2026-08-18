use tonic::{metadata::MetadataMap, Request, Status};

pub const METADATA_AUTHORIZATION: &str = "authorization";
pub const METADATA_X_AUTH_TOKEN: &str = "x-auth-token";
pub const METADATA_X_CLIENT_VERSION: &str = "x-client-version";

#[derive(Clone)]
pub struct AuthInterceptor {
    token: Option<String>,
}

impl AuthInterceptor {
    pub fn new(token: Option<String>) -> Self {
        Self {
            token: normalize(token.as_deref()),
        }
    }

    pub fn enabled(&self) -> bool {
        self.token.is_some()
    }
}

impl tonic::service::Interceptor for AuthInterceptor {
    fn call(&mut self, request: Request<()>) -> Result<Request<()>, Status> {
        let Some(expected) = self.token.as_deref() else {
            return Ok(request);
        };
        if tokens_match(expected, metadata_token(request.metadata()).as_deref()) {
            Ok(request)
        } else {
            Err(Status::unauthenticated(
                "missing or invalid token; send authorization: Bearer <CL_BROKER_AUTH_TOKEN>",
            ))
        }
    }
}

pub fn normalize(token: Option<&str>) -> Option<String> {
    token
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

pub fn tokens_match(expected: &str, presented: Option<&str>) -> bool {
    let Some(presented) = presented else {
        return false;
    };
    if expected.len() != presented.len() {
        return false;
    }
    expected
        .bytes()
        .zip(presented.bytes())
        .fold(0u8, |acc, (left, right)| acc | (left ^ right))
        == 0
}

pub fn bearer_or_raw(value: &str) -> &str {
    let value = value.trim();
    value
        .strip_prefix("Bearer ")
        .or_else(|| value.strip_prefix("bearer "))
        .unwrap_or(value)
        .trim()
}

pub fn metadata_client_version(metadata: &MetadataMap) -> String {
    metadata
        .get(METADATA_X_CLIENT_VERSION)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .unwrap_or_default()
}

pub fn metadata_token(metadata: &MetadataMap) -> Option<String> {
    if let Some(value) = metadata.get(METADATA_AUTHORIZATION).and_then(|v| v.to_str().ok()) {
        let token = bearer_or_raw(value);
        if !token.is_empty() {
            return Some(token.to_string());
        }
    }
    metadata
        .get(METADATA_X_AUTH_TOKEN)
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

pub fn http_token(authorization: Option<&str>, query: Option<&str>) -> Option<String> {
    if let Some(value) = authorization {
        let token = bearer_or_raw(value);
        if !token.is_empty() {
            return Some(token.to_string());
        }
    }
    query_token(query)
}

fn query_token(query: Option<&str>) -> Option<String> {
    for pair in query.unwrap_or_default().split('&') {
        let mut parts = pair.splitn(2, '=');
        if parts.next() == Some("token") {
            let value = parts.next().unwrap_or_default();
            if !value.is_empty() {
                return Some(value.to_string());
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::metadata::MetadataMap;

    #[test]
    fn accepts_bearer_and_raw() {
        assert_eq!(bearer_or_raw("Bearer secret"), "secret");
        assert_eq!(bearer_or_raw("secret"), "secret");
    }

    #[test]
    fn rejects_wrong_length() {
        assert!(!tokens_match("abc", Some("ab")));
        assert!(tokens_match("abc", Some("abc")));
        assert!(!tokens_match("abc", None));
    }

    #[test]
    fn reads_client_version_header() {
        let mut metadata = MetadataMap::new();
        assert_eq!(metadata_client_version(&metadata), "");
        metadata.insert(
            METADATA_X_CLIENT_VERSION,
            "java/0.1.29".parse().unwrap(),
        );
        assert_eq!(metadata_client_version(&metadata), "java/0.1.29");
    }
}
