use std::fs::File;
use std::io::Read;
use std::ops::{Deref, Range};

use arrow::buffer::Buffer;
use memmap2::Mmap;

/// Identifies a geoarrowrs index file.
const MAGIC: &[u8; 4] = b"GAIX";

/// Bumped whenever the layout below changes.
const VERSION: u32 = 1;

/// Magic, version, the row count, and the position count.
const HEADER_LEN: usize = 24;

/// Bytes owned in memory or mapped from a file, both reading as one slice.
type Bytes = Box<dyn Deref<Target = [u8]> + Send + Sync>;

/// An index in the layout `write()` saves: header, row positions, then the flatbush or kdbush tree on an 8 byte boundary.
pub(crate) struct IndexFile {
    bytes: Bytes,
    n: usize,
    positions: Range<usize>,
    tree: Range<usize>,
}

impl IndexFile {
    /// Lay out a freshly built tree with the row each of its entries came from.
    pub(crate) fn new(n: usize, positions: &[u32], tree: &[u8]) -> Self {
        let positions_end = HEADER_LEN + 4 * positions.len();
        let tree_start = positions_end.next_multiple_of(8);
        let len = tree_start + tree.len();

        // u64 words keep the buffer 8 byte aligned, which the tree's f64 boxes need
        let mut words = vec![0u64; len.div_ceil(8)];
        let out = bytemuck::cast_slice_mut::<u64, u8>(&mut words);
        out[0..4].copy_from_slice(MAGIC);
        out[4..8].copy_from_slice(&VERSION.to_le_bytes());
        out[8..16].copy_from_slice(&(n as u64).to_le_bytes());
        out[16..24].copy_from_slice(&(positions.len() as u64).to_le_bytes());
        for (dst, row) in out[HEADER_LEN..positions_end]
            .chunks_exact_mut(4)
            .zip(positions)
        {
            dst.copy_from_slice(&row.to_le_bytes());
        }
        out[tree_start..len].copy_from_slice(tree);

        Self {
            bytes: Box::new(Buffer::from_vec(words)),
            n,
            positions: HEADER_LEN..positions_end,
            tree: tree_start..len,
        }
    }

    /// Open a file `write()` saved, reading it into memory or mapping it.
    pub(crate) fn open(path: &str, mmap: bool) -> anyhow::Result<Self> {
        let mut file = File::open(path)?;

        if mmap {
            // a mapped file must not change while it is open, or reads are undefined behaviour
            let map = unsafe { Mmap::map(&file)? };
            let len = map.len();
            return Self::parse(Box::new(map) as Bytes, len);
        }

        let len = usize::try_from(file.metadata()?.len())?;
        let mut words = vec![0u64; len.div_ceil(8)];
        file.read_exact(&mut bytemuck::cast_slice_mut::<u64, u8>(&mut words)[..len])?;
        Self::parse(Box::new(Buffer::from_vec(words)) as Bytes, len)
    }

    /// Read the header of `len` bytes of index file, touching nothing past it.
    fn parse(bytes: Bytes, len: usize) -> anyhow::Result<Self> {
        let data = &bytes[..len];
        if len < HEADER_LEN || &data[0..4] != MAGIC {
            anyhow::bail!("Not a geoarrowrs index file");
        }

        let version = u32::from_le_bytes(data[4..8].try_into()?);
        if version != VERSION {
            anyhow::bail!("Unsupported index file version {version}, expected {VERSION}");
        }

        let n = usize::try_from(u64::from_le_bytes(data[8..16].try_into()?))?;
        let count = usize::try_from(u64::from_le_bytes(data[16..24].try_into()?))?;
        let positions_end = HEADER_LEN + 4 * count;
        let tree_start = positions_end.next_multiple_of(8);
        if tree_start > len {
            anyhow::bail!("The index file is truncated");
        }

        Ok(Self {
            bytes,
            n,
            positions: HEADER_LEN..positions_end,
            tree: tree_start..len,
        })
    }

    /// Save the index exactly as `open()` reads it.
    pub(crate) fn write(&self, path: &str) -> anyhow::Result<()> {
        std::fs::write(path, &self.bytes[..self.tree.end])?;
        Ok(())
    }

    /// The length of the array the index was built from.
    pub(crate) fn n(&self) -> usize {
        self.n
    }

    /// The original row of each tree entry, read in place; written little endian like the tree itself.
    pub(crate) fn positions(&self) -> &[u32] {
        bytemuck::cast_slice(&self.bytes[self.positions.clone()])
    }

    /// The tree's own flatbush or kdbush bytes.
    pub(crate) fn tree(&self) -> &[u8] {
        &self.bytes[self.tree.clone()]
    }
}
