use std::{iter::Zip, slice::Iter};
use crate::utils_share::cell::Cell;

#[derive(Debug, Clone, Default)]
pub struct Row {
    /// column names
    pub cols: Vec<String>,

    /// column cell list, should match with cols
    pub cells: Vec<Option<Cell>>,
}

impl Row {
    /// Create an empty row
    pub fn new() -> Self {
        Self::default()
    }

    /// Push a cell with column name to this row
    pub fn push(&mut self, col: &str, cell: Option<Cell>) {
        self.cols.push(col.to_owned());
        self.cells.push(cell);
    }
    
    pub fn iter(&self) -> Zip<Iter<'_, String>, Iter<'_, Option<Cell>>> {
        self.cols.iter().zip(self.cells.iter())
    }

    /// Remove a cell at the specified index
    pub fn retain<F>(&mut self, f: F)
    where
        F: FnMut((&String, &Option<Cell>)) -> bool,
    {
        let keep: Vec<bool> = self.iter().map(f).collect();
        let mut iter = keep.iter();
        self.cols.retain(|_| *iter.next().unwrap_or(&true));
        iter = keep.iter();
        self.cells.retain(|_| *iter.next().unwrap_or(&true));
    }
}
