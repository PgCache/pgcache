use std::{fmt, thread};

use nu_ansi_term::{Color, Style};
use tracing::{Event, Subscriber};
use tracing_subscriber::fmt::format::{FormatEvent, FormatFields, Writer};
use tracing_subscriber::registry::LookupSpan;

pub struct SimpeFormatter;

impl<S, N> FormatEvent<S, N> for SimpeFormatter
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        ctx: &tracing_subscriber::fmt::FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        let metadata = event.metadata();

        let span_name = ctx
            .current_span()
            .metadata()
            .map(|md| md.name())
            .unwrap_or_default();

        write!(
            &mut writer,
            "[{}]\t{} {} {}\t{}: ",
            metadata.level(),
            Style::new()
                .bold()
                .paint(thread::current().name().unwrap_or_default()),
            metadata.file().unwrap_or_default(),
            metadata.line().unwrap_or_default(),
            Color::Fixed(12).paint(span_name),
        )?;

        ctx.field_format().format_fields(writer.by_ref(), event)?;

        writeln!(writer)
    }
}
