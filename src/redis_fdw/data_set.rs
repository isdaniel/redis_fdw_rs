/// Result type for data loading operations
#[derive(Debug)]
pub enum LoadDataResult {
    /// Data was loaded and optimized with pushdown conditions
    PushdownApplied(Vec<String>),
    /// Data was loaded into internal storage without optimization
    LoadedToInternal,
    /// No data found or operation resulted in empty set
    Empty,
}

/// Represents the different states of data in a Redis table
#[derive(Debug, Clone, Default)]
pub enum DataSet {
    /// No data has been loaded yet
    #[default]
    Empty,
    /// Data loaded with pushdown optimization applied
    Filtered(Vec<String>),
    /// All data loaded without filtering
    Complete(DataContainer),
}

/// Container for complete data sets with type-specific storage
#[derive(Debug, Clone)]
pub enum DataContainer {
    /// Single string value (Redis String type)
    String(Option<String>),
    /// Key-value pairs (Redis Hash type)  
    Hash(Vec<(String, String)>),
    /// Ordered list of values (Redis List type)
    List(Vec<String>),
    /// Unordered set of values (Redis Set type)
    Set(Vec<String>),
    /// Sorted set with scores (Redis ZSet type)
    ZSet(Vec<(String, f64)>),
}

impl DataSet {
    /// Get the number of rows/items in this dataset
    pub fn len(&self) -> usize {
        match self {
            DataSet::Empty => 0,
            DataSet::Filtered(data) => {
                // Filtered data length depends on the data structure
                // This will be properly handled by the specific table type's get_row implementation
                data.len()
            },
            DataSet::Complete(container) => container.len(),
        }
    }

    /// Get a row at the specified index
    /// Note: For filtered data, this is a generic implementation
    /// Table types should override get_row to handle their specific data format
    pub fn get_row(&self, index: usize) -> Option<Vec<String>> {
        match self {
            DataSet::Empty => None,
            DataSet::Filtered(data) => {
                // Generic implementation - each element is a row
                data.get(index).map(|item| vec![item.clone()])
            },
            DataSet::Complete(container) => container.get_row(index),
        }
    }
}

impl DataContainer {
    /// Get the number of rows in this container
    pub fn len(&self) -> usize {
        match self {
            DataContainer::String(opt) => if opt.is_some() { 1 } else { 0 },
            DataContainer::Hash(pairs) => pairs.len(),
            DataContainer::List(items) => items.len(),
            DataContainer::Set(items) => items.len(),
            DataContainer::ZSet(items) => items.len(),
        }
    }

    /// Get a row at the specified index
    pub fn get_row(&self, index: usize) -> Option<Vec<String>> {
        match self {
            DataContainer::String(opt) => {
                if index == 0 && opt.is_some() {
                    opt.as_ref().map(|s| vec![s.clone()])
                } else {
                    None
                }
            },
            DataContainer::Hash(pairs) => {
                pairs.get(index).map(|(k, v)| vec![k.clone(), v.clone()])
            },
            DataContainer::List(items) => {
                items.get(index).map(|item| vec![item.clone()])
            },
            DataContainer::Set(items) => {
                items.get(index).map(|item| vec![item.clone()])
            },
            DataContainer::ZSet(items) => {
                items.get(index).map(|(member, score)| vec![member.clone(), score.to_string()])
            },
        }
    }
}
