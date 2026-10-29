//! Box CPU pressure (PGC-554): under sustained pressure the population pool
//! sheds, whatever its own queue wait says, for as long as each shed eases
//! the pressure; a shed that eases nothing is given back. The kernel's PSI is
//! stood in for by a PSI-format file the test rewrites; populations are
//! slowed so a burst of registrations breaches the pool's queue wait and
//! grows the pool first. Builds only with `--features fault-injection`.
#![cfg(feature = "fault-injection")]

use std::io::Error;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use crate::util::{MetricsSnapshot, TestContext, http_get};

mod util;

/// Pressure the monitor reads: PSI `some` percentages for the 10 s and 60 s
/// windows, and the box's waiting-task count.
fn pressure_write(
    path: &std::path::Path,
    avg10: f64,
    avg60: f64,
    waiting: f64,
) -> Result<(), Error> {
    std::fs::write(
        path,
        format!(
            "some avg10={avg10:.2} avg60={avg60:.2} avg300=0.00 total=0\nwaiting {waiting:.2}\n"
        ),
    )
}

async fn metrics_wait(
    ctx: &mut TestContext,
    what: &str,
    secs: u64,
    ready: impl Fn(&MetricsSnapshot) -> bool,
) -> Result<MetricsSnapshot, Error> {
    let deadline = Instant::now() + Duration::from_secs(secs);
    loop {
        let m = ctx.metrics().await?;
        if ready(&m) {
            return Ok(m);
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {what}: {m:?}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// The published `avg10` gauge, read from the raw exposition (gauges below
/// one are not representable in the integer snapshot).
async fn pressure_gauge(metrics_port: u16) -> Result<f64, Error> {
    let (_, body) = http_get(metrics_port, "/metrics").await?;
    Ok(body
        .lines()
        .find_map(|l| l.strip_prefix("pgcache_cache_cpu_pressure_avg10 "))
        .and_then(|v| v.trim().parse::<f64>().ok())
        .unwrap_or(-1.0))
}

/// Keep the population queue breached: distinct point queries, each a fresh
/// registration whose population the fault delays.
async fn registration_burst(ctx: &mut TestContext, from: u32, count: u32) -> Result<(), Error> {
    for id in from..from + count {
        ctx.simple_query(&format!("select id, v from pressured where id = {id}"))
            .await?;
    }
    Ok(())
}

/// A context with a PSI file at zero pressure and slow populations, its pool
/// grown above the floor by a registration storm. Returns the context, the
/// file, the floor, the grown size and the next free row id for more bursts.
async fn grown_pool_setup() -> Result<(TestContext, PathBuf, u64, u64, u32), Error> {
    let psi = std::env::temp_dir().join(format!(
        "pgcache-psi-{}-{}.txt",
        std::process::id(),
        Instant::now().elapsed().as_nanos()
    ));
    pressure_write(&psi, 0.0, 0.0, 0.0)?;
    let psi_path = psi.to_string_lossy().into_owned();
    let mut ctx = TestContext::setup_fault(&[
        ("PGCACHE_FAULT_CPU_PRESSURE_FILE", psi_path.as_str()),
        ("PGCACHE_FAULT_POPULATION_DELAY_MS", "300"),
    ])
    .await?;
    ctx.simple_query(
        "create table pressured (id int primary key, v int not null); \
         insert into pressured select g, g from generate_series(1, 4000) g",
    )
    .await?;
    ctx.cdc_decode_settle().await?;
    // The gauge is published on the controller's 1 s tick; under a loaded
    // suite the first tick can land after settle.
    let floor = metrics_wait(&mut ctx, "the pool's floor to be published", 15, |m| {
        m.cache_population_workers >= 1
    })
    .await?
    .cache_population_workers;

    // No pressure: a storm of slow populations breaches the queue wait and the
    // probe-and-verify controller grows the pool.
    let mut next_id = 1;
    let deadline = Instant::now() + Duration::from_secs(40);
    let grown = loop {
        registration_burst(&mut ctx, next_id, 20).await?;
        next_id += 20;
        tokio::time::sleep(Duration::from_millis(250)).await;
        let m = ctx.metrics().await?;
        if m.cache_population_workers > floor {
            break m;
        }
        assert!(
            Instant::now() < deadline,
            "pool never grew under the storm: {m:?}"
        );
    };
    assert_eq!(grown.cache_population_pressure_sheds, 0);
    Ok((ctx, psi, floor, grown.cache_population_workers, next_id))
}

/// Pressure the pool is causing: PSI is pinned at saturation the whole time
/// — it cannot tell the pool anything — while every member released takes one
/// task off the run queue, so each shed is confirmed and the pool walks down
/// to its floor while the storm, and its own breach, continue.
#[tokio::test]
async fn test_pressure_the_pool_causes_sheds_it_to_its_floor() -> Result<(), Error> {
    let (mut ctx, psi, floor, grown, mut next_id) = grown_pool_setup().await?;
    #[allow(clippy::cast_precision_loss)]
    let waiting_for = |workers: u64| 3.0 + workers as f64;
    pressure_write(&psi, 95.0, 95.0, waiting_for(grown))?;
    let deadline = Instant::now() + Duration::from_secs(120);
    let shed = loop {
        registration_burst(&mut ctx, next_id, 20).await?;
        next_id += 20;
        tokio::time::sleep(Duration::from_millis(250)).await;
        let m = ctx.metrics().await?;
        // The run queue eases as members leave: the file follows the live
        // count while PSI stays saturated.
        pressure_write(
            &psi,
            95.0,
            95.0,
            waiting_for(m.cache_population_workers.max(floor)),
        )?;
        if m.cache_population_workers <= floor && m.cache_population_pressure_sheds >= 1 {
            break m;
        }
        assert!(
            Instant::now() < deadline,
            "pool did not shed to its floor under pressure it causes: {m:?}"
        );
    };
    assert!(
        shed.cache_population_pressure_sheds >= grown - floor,
        "every member above the floor left as a shed"
    );
    assert_eq!(
        shed.cache_population_pressure_shed_restores, 0,
        "every shed eased the pressure: none restored"
    );
    tokio::time::sleep(Duration::from_millis(1_200)).await;
    assert!(
        (pressure_gauge(ctx.metrics_port).await? - 0.95).abs() < 1e-6,
        "the published gauge carries the injected reading"
    );

    // Still at the level, the storm cannot grow it back.
    registration_burst(&mut ctx, next_id, 80).await?;
    let held = metrics_wait(&mut ctx, "a tick under pressure", 5, |m| {
        m.cache_population_workers <= floor
    })
    .await?;
    assert_eq!(held.cache_population_workers, floor);

    let _ = std::fs::remove_file(&psi);
    Ok(())
}

/// Pressure the pool does not cause: PSI saturated and the run queue not
/// moving whatever the pool does, so the first shed is judged useless and the
/// member is given back.
#[tokio::test]
async fn test_pressure_the_pool_does_not_cause_restores_the_shed() -> Result<(), Error> {
    let (mut ctx, psi, floor, grown, mut next_id) = grown_pool_setup().await?;
    pressure_write(&psi, 95.0, 95.0, 20.0)?;
    let deadline = Instant::now() + Duration::from_secs(90);
    let restored = loop {
        registration_burst(&mut ctx, next_id, 20).await?;
        next_id += 20;
        tokio::time::sleep(Duration::from_millis(250)).await;
        let m = ctx.metrics().await?;
        if m.cache_population_pressure_shed_restores >= 1 {
            break m;
        }
        assert!(
            Instant::now() < deadline,
            "a useless shed was never restored: {m:?}"
        );
    };
    assert!(restored.cache_population_pressure_sheds >= 1);
    // The pool keeps (or regains) what it had; it is not walked to the floor.
    let kept = metrics_wait(&mut ctx, "the restored member", 15, |m| {
        m.cache_population_workers >= grown.min(floor + 1)
    })
    .await?;
    assert!(kept.cache_population_workers > floor, "{kept:?}");

    let _ = std::fs::remove_file(&psi);
    Ok(())
}
