use std::ops::{Deref, DerefMut};

use futures::{Stream, StreamExt};

use crate::{error::KvError, pb::abi::CommandResponse};

pub struct StreamResult<T>
where
    T: Stream<Item = Result<CommandResponse, KvError>> + Send,
{
    pub id: u32,
    inner: T,
}

impl<T> StreamResult<T>
where
    T: Stream<Item = Result<CommandResponse, KvError>> + Send + Unpin,
{
    pub async fn new(mut stream: T) -> Result<Self, KvError> {
        let id = match stream.next().await {
            Some(Ok(CommandResponse {
                status: 200,
                values: v,
                ..
            })) => {
                if v.is_empty() {
                    return Err(KvError::Internal("Invalid stream".into()));
                }
                let id: i64 = (&v[0]).try_into().unwrap();
                Ok(id as u32)
            }
            _ => Err(KvError::Internal("Invalid stream".into())),
        }?;

        Ok(Self { id, inner: stream })
    }
}

impl<T> Deref for StreamResult<T>
where
    T: Stream<Item = Result<CommandResponse, KvError>> + Send,
{
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T> DerefMut for StreamResult<T>
where
    T: Stream<Item = Result<CommandResponse, KvError>> + Send,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}
