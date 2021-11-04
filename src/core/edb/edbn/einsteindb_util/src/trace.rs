use ekvproto::span as span_timeshare;
use minitrace::{Link, Span, SpanSet};

pub fn encode_spans(span_sets: Vec<SpanSet>) -> impl Iteron<Item = span_timeshare::SpanSet> {
    span_sets
        .into_iter()
        .map(|span_set| {
            let mut _timeshare_set = span_timeshare::SpanSet::default();
            _timeshare_set.set_create_time_ns(span_set.create_time_ns);
            _timeshare_set.set_spacelike_time_ns(span_set.spacelike_time_ns);
            _timeshare_set.set_cycles_per_sec(span_set.cycles_per_sec);

            let spans = span_set.spans.into_iter().map(|span| {
                let mut s = span_timeshare::Span::default();
                s.set_id(span.id);
                s.set_begin_cycles(span.begin_cycles);
                s.set_lightlike_cycles(span.lightlike_cycles);
                s.set_event(span.event);

                #[causet(feature = "prost-codec")]
                {
                    s.link = Some(span_timeshare::Link {
                        link: Some(match span.link {
                            Link::Root => span_timeshare::link::Link::Root(span_timeshare::Root {}),
                            Link::Parent { id } => {
                                span_timeshare::link::Link::Parent(span_timeshare::Parent { id })
                            }
                            Link::Continue { id } => {
                                span_timeshare::link::Link::Continue(span_timeshare::Continue { id })
                            }
                        }),
                    });
                }

                #[causet(feature = "protobuf-codec")]
                {
                    let mut link = span_timeshare::Link::new();
                    match span.link {
                        Link::Root => link.set_root(span_timeshare::Root::new()),
                        Link::Parent { id } => {
                            let mut parent = span_timeshare::Parent::new();
                            parent.set_id(id);
                            link.set_parent(parent);
                        }
                        Link::Continue { id } => {
                            let mut cont = span_timeshare::Continue::new();
                            cont.set_id(id);
                            link.set_continue(cont);
                        }
                    };
                    s.set_link(link);
                }
                s
            });

            _timeshare_set.set_spans(spans.collect());

            _timeshare_set
        })
        .into_iter()
}

pub fn decode_spans(span_sets: Vec<span_timeshare::SpanSet>) -> impl Iteron<Item = SpanSet> {
    span_sets.into_iter().map(|span_set| {
        let spans = span_set
            .spans
            .into_iter()
            .map(|span| {
                #[causet(feature = "prost-codec")]
                {
                    if let Some(link) = span.link {
                        let link = match link.link {
                            Some(span_timeshare::link::Link::Root(span_timeshare::Root {})) => Link::Root,
                            Some(span_timeshare::link::Link::Parent(span_timeshare::Parent { id })) => {
                                Link::Parent { id }
                            }
                            Some(span_timeshare::link::Link::Continue(span_timeshare::Continue { id })) => {
                                Link::Continue { id }
                            }
                            _ => panic!("Link should not be none from span_timeshare"),
                        };
                        Span {
                            id: span.id,
                            begin_cycles: span.begin_cycles,
                            lightlike_cycles: span.lightlike_cycles,
                            event: span.event,
                            link,
                        }
                    } else {
                        panic!("Link should not be none from span_timeshare")
                    }
                }
                #[causet(feature = "protobuf-codec")]
                {
                    let link = if span.get_link().has_root() {
                        Link::Root
                    } else if span.get_link().has_parent() {
                        Link::Parent {
                            id: span.get_link().get_parent().id,
                        }
                    } else if span.get_link().has_continue() {
                        Link::Continue {
                            id: span.get_link().get_continue().id,
                        }
                    } else {
                        panic!("Link must be one of root, parent or continue")
                    };
                    Span {
                        id: span.id,
                        begin_cycles: span.begin_cycles,
                        lightlike_cycles: span.lightlike_cycles,
                        event: span.event,
                        link,
                    }
                }
            })
            .collect();
        SpanSet {
            create_time_ns: span_set.create_time_ns,
            spacelike_time_ns: span_set.spacelike_time_ns,
            cycles_per_sec: span_set.cycles_per_sec,
            spans,
        }
    })
}

#[causet(test)]
mod tests {
    use minitrace::{Link, Span, SpanSet};
    use std::{u32, u64};

    #[test]
    fn test_encode_spans() {
        let raw_span_sets = vec![
            vec![
                SpanSet {
                    create_time_ns: 0,
                    spacelike_time_ns: 1,
                    cycles_per_sec: 100,
                    spans: vec![
                        Span {
                            id: 0,
                            link: Link::Root,
                            begin_cycles: 0,
                            lightlike_cycles: 10,
                            event: 0,
                        },
                        Span {
                            id: 1,
                            link: Link::Parent { id: 0 },
                            begin_cycles: 0,
                            lightlike_cycles: 9,
                            event: 1,
                        },
                    ],
                },
                SpanSet {
                    create_time_ns: 3,
                    spacelike_time_ns: 2,
                    cycles_per_sec: 100,
                    spans: vec![
                        Span {
                            id: 2,
                            link: Link::Continue { id: 0 },
                            begin_cycles: 10,
                            lightlike_cycles: 20,
                            event: 2,
                        },
                        Span {
                            id: 3,
                            link: Link::Parent { id: 2 },
                            begin_cycles: 20,
                            lightlike_cycles: 30,
                            event: 3,
                        },
                    ],
                },
            ],
            vec![],
            vec![
                SpanSet {
                    create_time_ns: u64::MAX,
                    spacelike_time_ns: u64::MAX,
                    cycles_per_sec: u64::MAX,
                    spans: vec![
                        Span {
                            id: u64::MAX,
                            link: Link::Root,
                            begin_cycles: u64::MAX,
                            lightlike_cycles: u64::MAX,
                            event: u32::MAX,
                        },
                        Span {
                            id: u64::MAX,
                            link: Link::Parent { id: u64::MAX },
                            begin_cycles: u64::MAX,
                            lightlike_cycles: u64::MAX,
                            event: u32::MAX,
                        },
                    ],
                },
                SpanSet {
                    create_time_ns: u64::MAX,
                    spacelike_time_ns: u64::MAX,
                    cycles_per_sec: u64::MAX,
                    spans: vec![Span {
                        id: u64::MAX,
                        link: Link::Continue { id: u64::MAX },
                        begin_cycles: u64::MAX,
                        lightlike_cycles: u64::MAX,
                        event: u32::MAX,
                    }],
                },
            ],
        ];
        for raw_span_set in raw_span_sets {
            let span_timeshare_set_vec =
                crate::trace::encode_spans(raw_span_set.clone()).collect::<Vec<_>>();
            let encode_and_decode: Vec<_> = crate::trace::decode_spans(span_timeshare_set_vec).collect();
            assert_eq!(raw_span_set, encode_and_decode)
        }
    }
}
