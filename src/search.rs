use byteorder::{ByteOrder, LittleEndian};
use chrono::{DateTime, Utc};
use std::io::{Error, ErrorKind, Read, Seek, SeekFrom};

use crate::DecodeParams;
use crate::Timeseries;

#[derive(Debug)]
struct SearchBounds {
    start: u64,
    stop: u64,
}

#[derive(Debug)]
enum SbResult {
    Clipped,
    Bounded(SearchBounds),
}

#[derive(Debug)]
pub enum BoundResult {
    IoError(Error),
    NoData,
    Ok((u64, u64, DecodeParams)), // read_params, start_byte, stop_byte
}

impl Timeseries {
    //Search for start bounds
    //
    // Finds the requested TS, depending on if it is in the data do the following:
    //
    // case_A -- start_of_data -- case_B -- ?case_C?-- end_of_data -- ?case_C?--  case_D
    //
    // case A: requested TS before start of data
    //	-> CLIP [set read to start of file?]
    // case B: requested TS guaranteed within data
    //	-> SEARCH [largest header before B time, entry after B or EOF]
    // case C: requested TS might be within data or after
    //	-> SEARCH [largest header before B time, EOF]
    // case D: requested TS guaranteed outside of data
    //	-> ERROR
    //
    fn startread_search_bound(
        &mut self,
        start_time: DateTime<Utc>,
    ) -> Option<(SbResult, DecodeParams)> {
        log::debug!("header data {:?}", self.header.data);

        //get header timestamp =< timestamp, marks begin of search area
        if let Some(header_line) = self
            .header
            .data
            .range(..(start_time.timestamp() + 1))
            .next_back()
        {
            //Case B, C or D
            let start_search = *header_line.1;
            let start_timestamp = *header_line.0;

            //timestamp in header >= then sought timestamp, marks end of search area
            if let Some(header_line) = self.header.data.range(start_time.timestamp() + 1..).next() {
                //Case B -> return search area
                let next_timestamp = *header_line.0;
                let next_timestamp_pos = *header_line.1;
                let stop_search = next_timestamp_pos;
                Some((
                    SbResult::Bounded(SearchBounds {
                        start: start_search,
                        stop: stop_search,
                    }),
                    DecodeParams {
                        current_timestamp: start_timestamp,
                        next_timestamp,
                        next_timestamp_pos,
                    },
                ))
            } else {
                //Case C or D -> determine which
                if start_time <= self.last_time_in_data.unwrap() {
                    //TODO refactor remove unwrap could crash here
                    //Case C ->return search area clipped at EOF
                    let next_timestamp = i64::max_value() - 1;
                    //search at the most to the end of the file
                    let end_of_file = self.data.metadata().unwrap().len();
                    let stop_search = end_of_file.saturating_sub(self.full_line_size as u64);
                    //never switch to a new full timestamp as there are non
                    let next_timestamp_pos = end_of_file + 2; //TODO refactor try stop_search = next timestamp pos
                    Some((
                        SbResult::Bounded(SearchBounds {
                            start: start_search,
                            stop: stop_search,
                        }),
                        DecodeParams {
                            current_timestamp: start_timestamp,
                            next_timestamp,
                            next_timestamp_pos,
                        },
                    ))
                } else {
                    log::debug!(
                        "start_time: {}, last_in_data: {:?}",
                        start_time,
                        self.last_time_in_data
                    );
                    //Case D -> no data within user requested interval
                    None
                }
            }
        } else {
            //Case A -> clip to start of file
            log::warn!("start TS earlier then start of data -> start_byte = 0");
            //there should always be a header in a non empty file, thus if start_time results in
            //Case A then the following cant fail.
            let header_line = self
                .header
                .data
                .range(start_time.timestamp() + 1..)
                .next()
                .expect("no header found, these should always be one header! datafile is corrupt");
            //get the start timestamp from this header
            let start_timestamp = *header_line.0;

            //check if there is another header
            let decode_params =
                if let Some(header_line) = self.header.data.range(start_timestamp + 1..).next() {
                    let next_timestamp = *header_line.0;
                    let next_timestamp_pos = *header_line.1;
                    DecodeParams {
                        current_timestamp: start_timestamp,
                        next_timestamp,
                        next_timestamp_pos,
                    }
                } else {
                    //use safe defaults
                    let end_of_file = self.data.metadata().unwrap().len();
                    let next_timestamp = i64::max_value() - 1; //-1 prevents overflow
                    let next_timestamp_pos = end_of_file + 2; //+2 makes sure we never switch to the next timestamp
                    DecodeParams {
                        current_timestamp: start_timestamp,
                        next_timestamp,
                        next_timestamp_pos,
                    }
                };

            Some((SbResult::Clipped, decode_params))
        }
    }

    fn find_read_start(
        &mut self,
        start_time: DateTime<Utc>,
        search_params: SearchBounds,
    ) -> Result<u64, Error> {
        //compare partial (16 bit) timestamps in between the bounds
        let mut buf = vec![0u8; (search_params.stop - search_params.start) as usize];
        self.data.seek(SeekFrom::Start(search_params.start))?;
        self.data.read_exact(&mut buf)?;

        for line_start in (0..buf.len().saturating_sub(2)).step_by(self.full_line_size) {
            if LittleEndian::read_u16(&buf[line_start..line_start + 2])
                >= start_time.timestamp() as u16
            {
                log::debug!("setting start_byte from liniar search, pos: {}", line_start);
                let start_byte = search_params.start + line_start as u64;
                return Ok(start_byte);
            }
        }

        //no data more recent then start time within bounds, return location of most recent data
        Ok(search_params.stop)
    }

    //Search for stop bounds
    //
    // Finds the requested TS, depending on if it is in the data do the following:
    //
    // case_A -- start_of_data -- case_B -- ?case_C?-- end_of_data -- ?case_C?--  case_D
    //
    // case A: requested TS before start of data
    //	-> ERROR, no data can possibly be read now
    // case B: requested TS guaranteed within data
    //	-> SEARCH [largest header before B time, entry after B or EOF]
    // case C: requested TS might be within data or after
    //	-> SEARCH [largest header before B time, EOF]
    // case D: requested TS guaranteed outside of data
    //	-> CLIP, clipping to end
    //
    fn stopread_search_bounds(&mut self, start_time: DateTime<Utc>) -> Option<SbResult> {
        log::debug!("header data {:?}", self.header.data);
        //get header timestamp =< timestamp, marks begin of search area
        if let Some(header_line) = self
            .header
            .data
            .range(..(start_time.timestamp() + 1))
            .next_back()
        {
            //Case B, C or D
            let start_search = *header_line.1;

            //timestamp in header >= then sought timestamp, marks end of search area
            if let Some(header_line) = self.header.data.range(start_time.timestamp() + 1..).next() {
                //Case B -> return search area
                let next_timestamp_pos = *header_line.1;
                let stop_search = next_timestamp_pos;
                Some(SbResult::Bounded(SearchBounds {
                    start: start_search,
                    stop: stop_search,
                }))
            } else {
                //Case D or C -> determine which
                if start_time <= self.last_time_in_data.unwrap() {
                    //TODO refactor handle unwrap crash
                    //Case D ->return search area
                    //search at the most to the end of the file
                    let end_of_file = self.data.metadata().unwrap().len();
                    let stop_search = end_of_file.saturating_sub(self.full_line_size as u64);
                    Some(SbResult::Bounded(SearchBounds {
                        start: start_search,
                        stop: stop_search,
                    }))
                } else {
                    //Case D
                    Some(SbResult::Clipped)
                }
            }
        } else {
            //Case A -> ERROR
            log::warn!("start TS earlier then start of data -> start_byte = 0");
            None
        }
    }

    pub fn get_bounds(
        &mut self,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> BoundResult {
        //check if the datafile isnt empty

        if self.data_size == 0 {
            return BoundResult::NoData;
        }

        let start_bounds = self.startread_search_bound(start_time);
        if start_bounds.is_none() {
            let error = Error::new(
                ErrorKind::NotFound,
                "start_time TS more recent then last data",
            );
            return BoundResult::IoError(error);
        }

        let stop_bounds = self.stopread_search_bounds(end_time);
        if stop_bounds.is_none() {
            let error = Error::new(ErrorKind::NotFound, "end_time older then oldest data");
            return BoundResult::IoError(error);
        }

        //must be a solvable request
        let (case, decode_params) = start_bounds.unwrap();
        let start_byte = match case {
            SbResult::Bounded(search_bounds) => {
                //TODO change to use ? operator
                let start_byte = self.find_read_start(start_time, search_bounds);
                if let Err(err) = start_byte {
                    return BoundResult::IoError(err);
                }
                start_byte.unwrap()
            }
            SbResult::Clipped => 0,
        };

        let case = stop_bounds.unwrap();
        let stop_byte = match case {
            SbResult::Bounded(search_bounds) => {
                //TODO change to use ? operator
                let stop_byte = self.find_read_stop(end_time, search_bounds);
                if let Err(err) = stop_byte {
                    return BoundResult::IoError(err);
                }
                stop_byte.unwrap()
            }
            SbResult::Clipped => {
                let end_of_file = self.data.metadata().unwrap().len();
                end_of_file.saturating_sub(self.full_line_size as u64)
            }
        };

        log::debug!(
            "start time: {}, {}; end_time: {}, {}",
            start_time,
            start_time.timestamp(),
            end_time,
            end_time.timestamp()
        );
        log::debug!("start_byte: {}", start_byte);

        BoundResult::Ok((start_byte, stop_byte, decode_params))
    }

    fn find_read_stop(
        &mut self,
        end_time: DateTime<Utc>,
        search_params: SearchBounds,
    ) -> Result<u64, Error> {
        //compare partial (16 bit) timestamps in between these bounds
        let mut buf = vec![0u8; (search_params.stop - search_params.start) as usize];
        self.data.seek(SeekFrom::Start(search_params.start))?;
        self.data.read_exact(&mut buf)?;

        log::trace!("buf.len(): {}", buf.len());
        for line_start in (0..buf.len() - self.full_line_size + 1)
            .rev()
            .step_by(self.full_line_size)
        {
            //trace!("line: {}, {}", line_start, LittleEndian::read_u16(&buf[line_start..line_start + 2]));
            if LittleEndian::read_u16(&buf[line_start..line_start + 2])
                <= end_time.timestamp() as u16
            {
                log::debug!("setting start_byte from liniar search, start of search area");
                let stop_byte = search_params.start + line_start as u64;
                return Ok(stop_byte);
            }
        }
        Ok(search_params.stop)
    }
}
