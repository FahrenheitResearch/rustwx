use rustwx_core::{ModelId, SourceId};
use rustwx_models::LatestRun;
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::io;
use std::thread;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreparedRunMetadata {
    pub model: ModelId,
    pub date_yyyymmdd: String,
    pub cycle_utc: u8,
    pub forecast_hour: u16,
    pub source: SourceId,
}

impl PreparedRunMetadata {
    pub fn from_latest(latest: &LatestRun, forecast_hour: u16) -> Self {
        Self {
            model: latest.model,
            date_yyyymmdd: latest.cycle.date_yyyymmdd.clone(),
            cycle_utc: latest.cycle.hour_utc,
            forecast_hour,
            source: latest.source,
        }
    }
}

#[derive(Debug)]
pub struct PreparedRunContext<C> {
    metadata: PreparedRunMetadata,
    context: C,
}

impl<C> PreparedRunContext<C> {
    pub fn new(metadata: PreparedRunMetadata, context: C) -> Self {
        Self { metadata, context }
    }

    pub fn metadata(&self) -> &PreparedRunMetadata {
        &self.metadata
    }

    pub fn context(&self) -> &C {
        &self.context
    }

    pub fn into_parts(self) -> (PreparedRunMetadata, C) {
        (self.metadata, self.context)
    }
}

pub struct FanoutLane<'scope, T> {
    name: &'static str,
    job: Box<dyn FnOnce() -> Result<T, Box<dyn Error>> + Send + 'scope>,
}

impl<'scope, T> FanoutLane<'scope, T> {
    pub fn new<F>(name: &'static str, job: F) -> Self
    where
        F: FnOnce() -> Result<T, Box<dyn Error>> + Send + 'scope,
    {
        Self {
            name,
            job: Box::new(job),
        }
    }

    fn run(self) -> Result<T, Box<dyn Error>> {
        (self.job)()
    }
}

pub fn lane<'scope, T, F>(name: &'static str, job: F) -> FanoutLane<'scope, T>
where
    F: FnOnce() -> Result<T, Box<dyn Error>> + Send + 'scope,
{
    FanoutLane::new(name, job)
}

pub fn run_fanout3<'scope, A, B, C>(
    concurrent: bool,
    first: Option<FanoutLane<'scope, A>>,
    second: Option<FanoutLane<'scope, B>>,
    third: Option<FanoutLane<'scope, C>>,
) -> Result<(Option<A>, Option<B>, Option<C>), Box<dyn Error>>
where
    A: Send + 'scope,
    B: Send + 'scope,
    C: Send + 'scope,
{
    if concurrent {
        thread::scope(|scope| {
            let first_handle = first.map(|lane| {
                let name = lane.name;
                (
                    name,
                    scope.spawn(move || lane.run().map_err(|err| lane_error(name, err))),
                )
            });
            let second_handle = second.map(|lane| {
                let name = lane.name;
                (
                    name,
                    scope.spawn(move || lane.run().map_err(|err| lane_error(name, err))),
                )
            });
            let third_handle = third.map(|lane| {
                let name = lane.name;
                (
                    name,
                    scope.spawn(move || lane.run().map_err(|err| lane_error(name, err))),
                )
            });

            let first = first_handle
                .map(|(name, handle)| join_lane(name, handle))
                .transpose()?;
            let second = second_handle
                .map(|(name, handle)| join_lane(name, handle))
                .transpose()?;
            let third = third_handle
                .map(|(name, handle)| join_lane(name, handle))
                .transpose()?;

            Ok::<_, Box<dyn Error>>((first, second, third))
        })
    } else {
        Ok((
            first.map(FanoutLane::run).transpose()?,
            second.map(FanoutLane::run).transpose()?,
            third.map(FanoutLane::run).transpose()?,
        ))
    }
}

fn join_lane<T>(
    name: &'static str,
    handle: thread::ScopedJoinHandle<'_, io::Result<T>>,
) -> Result<T, Box<dyn Error>> {
    match handle.join() {
        Ok(result) => result.map_err(Box::<dyn Error>::from),
        Err(panic) => Err(Box::new(io::Error::other(format!(
            "{name} lane panicked: {}",
            panic_message(panic)
        )))),
    }
}

fn lane_error(name: &'static str, err: Box<dyn Error>) -> io::Error {
    io::Error::other(format!("{name} lane failed: {err}"))
}

fn panic_message(panic: Box<dyn std::any::Any + Send + 'static>) -> String {
    if let Some(message) = panic.downcast_ref::<&'static str>() {
        (*message).to_string()
    } else if let Some(message) = panic.downcast_ref::<String>() {
        message.clone()
    } else {
        "unknown panic".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustwx_core::CycleSpec;

    #[test]
    fn prepared_run_metadata_tracks_latest_run_identity() {
        let latest = LatestRun {
            model: ModelId::Hrrr,
            cycle: CycleSpec::new("20260415", 18).unwrap(),
            source: SourceId::Nomads,
        };
        let metadata = PreparedRunMetadata::from_latest(&latest, 12);
        assert_eq!(metadata.model, ModelId::Hrrr);
        assert_eq!(metadata.date_yyyymmdd, "20260415");
        assert_eq!(metadata.cycle_utc, 18);
        assert_eq!(metadata.forecast_hour, 12);
        assert_eq!(metadata.source, SourceId::Nomads);
    }

    #[test]
    fn fanout_runs_sequential_lanes_without_threads() {
        let (first, second, third) = run_fanout3(
            false,
            Some(lane("first", || Ok::<_, Box<dyn Error>>(1usize))),
            Some(lane("second", || {
                Ok::<_, Box<dyn Error>>("two".to_string())
            })),
            None::<FanoutLane<'_, ()>>,
        )
        .unwrap();
        assert_eq!(first, Some(1));
        assert_eq!(second.as_deref(), Some("two"));
        assert_eq!(third, None);
    }

    #[test]
    fn fanout_runs_concurrent_lanes_and_preserves_outputs() {
        let (first, second, third) = run_fanout3(
            true,
            Some(lane("first", || Ok::<_, Box<dyn Error>>(3usize))),
            None::<FanoutLane<'_, String>>,
            Some(lane("third", || Ok::<_, Box<dyn Error>>(9u8))),
        )
        .unwrap();
        assert_eq!(first, Some(3));
        assert_eq!(second, None);
        assert_eq!(third, Some(9));
    }
}
