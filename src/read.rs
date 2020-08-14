use std::io::Error;

use crate::DecodeParams;
use crate::Timeseries;

#[derive(Debug)]
pub struct Selector {
    spacing: u64, //in lines
    counter: u64, //starts at 1
    current: u64, //starts at 0

    full_line_size: usize,
    pub lines_per_sample: std::num::NonZeroUsize,
    //binsize; usize//in lines
}

impl Selector {
    //
    pub fn new(max_plot_points: usize, numb_lines: u64, timeseries: &Timeseries) -> Option<Self> {
        if numb_lines <= max_plot_points as u64 {
            return None;
        }

        let full_line_size = timeseries.full_line_size;
        let lines_to_skip: u64 = numb_lines % max_plot_points as u64;

        dbg!(numb_lines);
        dbg!(lines_to_skip);
        dbg!(max_plot_points);
        let lines_per_sample =
            std::num::NonZeroUsize::new((numb_lines / max_plot_points as u64) as usize).unwrap();

        Some(Self {
            spacing: ((numb_lines - lines_to_skip) as u64) / lines_to_skip,
            counter: 1,
            current: 0,
            full_line_size,
            lines_per_sample,
        })
    }

    //calculate if element with index idx should be used
    fn use_index(&mut self) -> bool {
        if self.current == self.counter * self.spacing {
            self.counter += 1;
            //dont increment the current counter as we will skip this point
            false
        } else {
            self.current += 1;
            true
        }
    }

    //one and a halve spacing
    fn n_to_skip(&self, lines_to_read: usize) -> usize {
        let stop_pos: u64 = self.current + lines_to_read as u64; //can we use current though? what happens after skip?
        let first_skip_pos: u64 = self.counter * self.spacing;

        dbg!(stop_pos);
        //check if skip in this read chunk
        if first_skip_pos > stop_pos {
            dbg!(0);
            0
        } else {
            //there is at least one skip, check if there are more
            let numb_of_additional_skips =
                (stop_pos.saturating_sub(first_skip_pos) / self.spacing) as usize;
            dbg!(stop_pos.saturating_sub(first_skip_pos));
            dbg!(1 + numb_of_additional_skips);
            1 + numb_of_additional_skips
        }
    }
}

impl Timeseries {
    pub fn decode_time_into_given(
        &mut self,
        timestamps: &mut Vec<u64>,
        line_data: &mut Vec<u8>,
        lines_to_read: usize,
        start_byte: &mut u64,
        stop_byte: u64,
        decode_params: &mut DecodeParams,
    ) -> Result<(), Error> {
        //let mut buf = Vec::with_capacity(lines_to_read*self.full_line_size);
        let mut buf = vec![0; lines_to_read * self.full_line_size];
        timestamps.clear();
        line_data.clear();

        //save file pos indicator before read call moves it around
        let file_pos = *start_byte;
        let n_read = self.read(&mut buf, start_byte, stop_byte)? as usize;
        log::trace!("read: {} bytes", n_read);
        for (line, file_pos) in buf[..n_read]
            .chunks(self.full_line_size)
            .zip((file_pos..).step_by(self.full_line_size))
        {
            timestamps.push(self.get_timestamp::<u64>(line, file_pos, decode_params));
            line_data.extend_from_slice(&line[2..]);
        }
        Ok(())
    }
    //based on https://github.com/dskleingeld/HomeAutomation/blob/7022c5c65f758762a666fc43303f13fecba28100/pi_Cpp/dataStorage/MainData.cpp
    pub fn decode_time_into_given_skipping(
        &mut self,
        timestamps: &mut Vec<u64>,
        line_data: &mut Vec<u8>,
        lines_to_read: usize,
        start_byte: &mut u64,
        stop_byte: u64,
        decode_params: &mut DecodeParams,
        selector: &mut Selector,
    ) -> Result<(), Error> {
        //let mut buf = Vec::with_capacity(lines_to_read*self.full_line_size);
        let lines_to_skip = selector.n_to_skip(lines_to_read);
        let mut buf = vec![0; (lines_to_read + lines_to_skip) * self.full_line_size]; //TODO FIXME
        timestamps.clear();
        line_data.clear();

        //save file pos indicator before read call moves it around
        let file_pos = *start_byte;
        let n_read = self.read(&mut buf, start_byte, stop_byte)? as usize;
        log::trace!("read: {} bytes", n_read);
        dbg!(n_read);
        for (line, file_pos) in buf[..n_read]
            .chunks(self.full_line_size)
            .zip((file_pos..).step_by(self.full_line_size))
            .filter(|_| selector.use_index())
        {
            timestamps.push(self.get_timestamp::<u64>(line, file_pos, decode_params));
            line_data.extend_from_slice(&line[2..]);
        }
        Ok(())
    }
}
