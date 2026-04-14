// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use arrow_array::{ArrayRef, Int32Array, RecordBatch};
use tracing::{Id, Instrument, Subscriber};
use tracing_subscriber::layer::{Context, Layer};
use tracing_subscriber::prelude::*;
use tracing_subscriber::registry::LookupSpan;

use super::test_scan;
use crate::utils::build_multi_fragment_dataset;

const ROW_COUNT: i32 = 12;
const VALUE_OFFSET: i32 = 100;

#[derive(Clone, Debug)]
struct RecordedSpan {
    name: String,
    parent: Option<Id>,
    closed: bool,
}

#[derive(Clone, Default, Debug)]
struct SpanTreeRecorder {
    spans: Arc<Mutex<HashMap<Id, RecordedSpan>>>,
}

impl SpanTreeRecorder {
    fn snapshot(&self) -> HashMap<Id, RecordedSpan> {
        self.spans.lock().unwrap().clone()
    }
}

impl<S> Layer<S> for SpanTreeRecorder
where
    S: Subscriber + for<'span> LookupSpan<'span>,
{
    fn on_new_span(&self, attrs: &tracing::span::Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let parent = attrs.parent().cloned().or_else(|| {
            if attrs.is_contextual() {
                ctx.current_span().id().cloned()
            } else {
                None
            }
        });

        let span = RecordedSpan {
            name: attrs.metadata().name().to_string(),
            parent,
            closed: false,
        };
        self.spans.lock().unwrap().insert(id.clone(), span);
    }

    fn on_close(&self, id: Id, _ctx: Context<'_, S>) {
        if let Some(span) = self.spans.lock().unwrap().get_mut(&id) {
            span.closed = true;
        }
    }
}

fn multi_fragment_batch() -> RecordBatch {
    let ids = Int32Array::from((0..ROW_COUNT).collect::<Vec<_>>());
    let values = Int32Array::from((VALUE_OFFSET..(VALUE_OFFSET + ROW_COUNT)).collect::<Vec<_>>());

    RecordBatch::try_from_iter(vec![
        ("id", Arc::new(ids) as ArrayRef),
        ("value", Arc::new(values) as ArrayRef),
    ])
    .unwrap()
}

fn assert_descends_from_root(spans: &HashMap<Id, RecordedSpan>, span_id: &Id, root_id: &Id) {
    let mut current_id = span_id.clone();
    let mut visited = HashSet::new();

    loop {
        if &current_id == root_id {
            return;
        }

        assert!(
            visited.insert(current_id.clone()),
            "Cycle detected in span parent chain for {:?}",
            span_id
        );

        let span = spans
            .get(&current_id)
            .unwrap_or_else(|| panic!("Missing recorded span for {:?}", current_id));
        let parent = span.parent.clone().unwrap_or_else(|| {
            panic!(
                "Span '{}' was orphaned before reaching test root",
                span.name
            )
        });
        current_id = parent;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn test_multi_fragment_scan_does_not_create_orphan_spans() {
    let original = multi_fragment_batch();
    let ds = build_multi_fragment_dataset(original.clone()).await;

    let recorder = SpanTreeRecorder::default();
    let subscriber = tracing_subscriber::registry().with(recorder.clone());
    let _guard = tracing::subscriber::set_default(subscriber);

    let root_span = tracing::info_span!("lance_query_test_root");
    let root_id = root_span
        .id()
        .expect("root span should be recorded by the test subscriber");

    async {
        test_scan(&original, &ds).await;
    }
    .instrument(root_span.clone())
    .await;

    drop(root_span);
    tokio::task::yield_now().await;

    let spans = recorder.snapshot();
    assert!(spans.contains_key(&root_id), "root span was not recorded");

    let descendants = spans
        .keys()
        .filter_map(|id| {
            if id == &root_id {
                None
            } else {
                Some(id.clone())
            }
        })
        .collect::<Vec<_>>();
    assert!(
        !descendants.is_empty(),
        "query produced no descendant spans under the test root"
    );

    for span_id in &descendants {
        assert_descends_from_root(&spans, span_id, &root_id);
    }

    let open_spans = spans
        .values()
        .filter(|span| !span.closed)
        .map(|span| span.name.clone())
        .collect::<Vec<_>>();
    assert!(
        open_spans.is_empty(),
        "spans left open after query finished: {:?}",
        open_spans
    );
}
