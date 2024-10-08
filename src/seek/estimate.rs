use crate::series::data::index::{EndArea, PayloadSize, StartArea};

use super::RoughPos;

#[derive(Debug)]
pub(crate) struct Estimate {
    pub(crate) max: u64,
    pub(crate) min: u64,
}

impl RoughPos {
    pub(crate) fn estimate_lines(
        &self,
        payload_size: PayloadSize,
        data_len: u64,
    ) -> Estimate {
        use EndArea as End;
        use StartArea::{Clipped, Found, Gap, TillEnd, Window};

        let estimate_in_bytes =
            match (self.start_search_area.clone(), self.end_search_area.clone()) {
                (Found(start) | Gap { stops: start }, End::Found(end)) => Estimate {
                    max: end - start,
                    min: end - start,
                },
                (Found(start) | Gap { stops: start }, End::Gap { start: end }) => {
                    Estimate {
                        max: end.raw_offset() - start.raw_offset(),
                        min: end.raw_offset() - start.raw_offset(),
                    }
                }
                (Found(start) | Gap { stops: start }, End::TillEnd(end)) => Estimate {
                    max: data_len - start.0,
                    min: end - start,
                },
                (Found(start) | Gap { stops: start }, End::Window(end_min, end_max)) => {
                    Estimate {
                        max: end_max.raw_offset() - start.raw_offset(),
                        min: end_min - start,
                    }
                }

                (Clipped, End::Found(end)) => Estimate {
                    max: end.raw_offset(),
                    min: end.raw_offset(),
                },
                (Clipped, End::Gap { start: end }) => Estimate {
                    max: end.raw_offset(),
                    // incorrect, but we have no better guess for it
                    min: end.raw_offset(),
                },
                (Clipped, End::TillEnd(end)) => Estimate {
                    max: data_len,
                    min: end.raw_offset(),
                },
                (Clipped, End::Window(end_min, end_max)) => Estimate {
                    max: end_max.raw_offset(),
                    min: end_min.raw_offset(),
                },

                (TillEnd(start), End::Found(end)) => Estimate {
                    max: end - start,
                    min: 1,
                },
                (TillEnd(start), End::Gap { start: end }) => Estimate {
                    max: end.line_start(payload_size) - start,
                    min: 1,
                },
                (TillEnd(start), End::TillEnd(_)) => Estimate {
                    max: data_len - start.raw_offset(),
                    min: 1,
                },
                (TillEnd(_), End::Window(_, _)) => unreachable!(
                "The start has to lie before the end, if the end is a search area from \
                min..max then start can not be an area from start..end_of_file"
            ),

                (Window(start_min, start_max), End::Found(end)) => Estimate {
                    max: end - start_min,
                    min: end - start_max.line_start(payload_size),
                },
                (Window(start_min, start_max), End::Gap { start: end }) => Estimate {
                    max: end.raw_offset() - start_min.raw_offset(),
                    min: end - start_max,
                },
                (Window(start_min, start_max), End::TillEnd(end)) => Estimate {
                    max: data_len - start_min.raw_offset(),
                    min: end - start_max.line_start(payload_size),
                },
                (Window(start_min, start_max), End::Window(end_min, end_max)) => {
                    Estimate {
                        max: end_max.raw_offset() - start_min.raw_offset(),
                        min: end_min - start_max.line_start(payload_size),
                    }
                }
            };

        Estimate {
            max: estimate_in_bytes.max / payload_size.line_size() as u64,
            min: estimate_in_bytes.min / payload_size.line_size() as u64,
        }
    }
}
