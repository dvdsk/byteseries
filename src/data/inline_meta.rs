use core::fmt;
use std::io::{Read, Seek, SeekFrom, Write};
use std::iter;

use itertools::Itertools;

#[derive(Debug)]
pub(crate) struct FileWithInlineMeta<F: fmt::Debug> {
    pub(crate) file_handle: F,
    pub(crate) full_line_size: usize,
}


pub(crate) fn meta_lines_indices(buf: &[u8], full_line_size: usize) -> Vec<usize> {
    buf.chunks_exact(full_line_size)
        .enumerate()
        .tuple_windows::<(_, _)>()
        .filter(|((_, line_a), _)| line_a[0..2] == [0, 0])
        .filter(|(_, (_, line_b))| line_b[0..2] == [0, 0])
        .map(|((line_numb, _), (_, _))| line_numb)
        .map(|line| line * full_line_size)
        .collect()
}

pub(crate) fn lines_per_metainfo(line_size: usize) -> usize {
    let base_lines = 2; // needed to recognise meta section
    let extra_lines_needed = match line_size {
        0 => 2,
        1 => 2,
        2 => 1,
        3 => 1,
        4.. => 0,
    };
    base_lines + extra_lines_needed
}

fn shift_over_meta_lines(buf: &mut [u8], meta_lines: Vec<usize>, full_line_size: usize) -> usize {
    let n_meta_lines = dbg!(lines_per_metainfo(full_line_size - 2));
    let n_meta_bytes = n_meta_lines * full_line_size;
    let mut shifted = 0;
    let total_to_shift = meta_lines.len() * n_meta_bytes;
    for (start, end) in meta_lines
        .into_iter()
        .chain(iter::once(buf.len() - total_to_shift))
        .tuple_windows::<(_, _)>()
    {
        for i in start..end {
            buf[i] = buf[i + n_meta_bytes + shifted];
        }
        shifted += n_meta_bytes;
    }
    shifted
}

impl<F: fmt::Debug> FileWithInlineMeta<F> {
    pub(crate) fn inner_mut(&mut self) -> &mut F {
        &mut self.file_handle
    }
}

impl<F: Read + fmt::Debug> Read for FileWithInlineMeta<F> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let mut read_ignoring_meta = 0;

        while read_ignoring_meta == 0 {
            let n_read = self.file_handle.read(buf)?;
            let mut read = &mut buf[..n_read];
            if read.is_empty() {
                return Ok(0);
            }

            let meta_lines = meta_lines_indices(read, self.full_line_size);
            let shifted = shift_over_meta_lines(&mut read, meta_lines, self.full_line_size);

            read_ignoring_meta = n_read - shifted;
        }

        Ok(read_ignoring_meta)
    }
}

impl<F: Write + fmt::Debug> Write for FileWithInlineMeta<F> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.file_handle.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.file_handle.flush()
    }
}

impl<F: Seek + fmt::Debug> Seek for FileWithInlineMeta<F> {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        self.file_handle.seek(pos)
    }
}

#[cfg(test)]
mod test {
    use std::io::Cursor;
    use std::io::Read;

    use super::*;
    const FULL_LINE_SIZE: usize = 6;
    const TWO_ZERO_LINES: [u8; 2 * FULL_LINE_SIZE] = [0u8; 2 * FULL_LINE_SIZE];

    fn data_lines<const N: usize>(n: usize) -> Vec<u8> {
        assert!(N >= 2);
        (1..(n + 1))
            .into_iter()
            .map(|i| {
                let mut line = [0u8; N];
                line[0] = (i % u8::MAX as usize) as u8;
                line[1] = (i / u8::MAX as usize) as u8;
                line
            })
            .flatten()
            .collect()
    }

    #[test]
    fn meta_section_at_start() {
        let n_data_lines = 5;
        let mut lines = TWO_ZERO_LINES.to_vec();
        lines.extend_from_slice(&data_lines::<FULL_LINE_SIZE>(n_data_lines));

        let file = Cursor::new(lines);
        let mut file = FileWithInlineMeta {
            file_handle: file,
            full_line_size: FULL_LINE_SIZE,
        };

        let mut buf = vec![0u8; 100];
        let n_read = file.read(&mut buf).unwrap();
        let read = &buf[0..n_read];
        assert_eq!(read, &data_lines::<FULL_LINE_SIZE>(n_data_lines))
    }

    #[test]
    fn meta_section_at_end() {
        let n_data_lines = 5;
        let mut lines = data_lines::<FULL_LINE_SIZE>(n_data_lines);
        lines.extend_from_slice(&TWO_ZERO_LINES);

        let file = Cursor::new(lines);
        let mut file = FileWithInlineMeta {
            file_handle: file,
            full_line_size: FULL_LINE_SIZE,
        };

        let mut buf = vec![0u8; 100];
        let n_read = file.read(&mut buf).unwrap();
        let read = &buf[0..n_read];
        assert_eq!(read, &data_lines::<FULL_LINE_SIZE>(n_data_lines))
    }

    #[test]
    fn meta_sections_around() {
        let n_data_lines = 2;
        let mut lines = TWO_ZERO_LINES.to_vec();
        lines.extend_from_slice(&data_lines::<FULL_LINE_SIZE>(n_data_lines));
        lines.extend_from_slice(&TWO_ZERO_LINES);

        let file = Cursor::new(lines);
        let mut file = FileWithInlineMeta {
            file_handle: file,
            full_line_size: FULL_LINE_SIZE,
        };

        let mut buf = vec![0u8; 100];
        let n_read = file.read(&mut buf).unwrap();
        let read = &buf[0..n_read];
        assert_eq!(read, &data_lines::<FULL_LINE_SIZE>(n_data_lines))
    }
}
