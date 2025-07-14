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
}
