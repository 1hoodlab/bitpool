/// Shared error type for repository operations (domain layer only).
#[derive(Debug)]
pub struct DomainRepoError(pub String);

impl std::fmt::Display for DomainRepoError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for DomainRepoError {}
