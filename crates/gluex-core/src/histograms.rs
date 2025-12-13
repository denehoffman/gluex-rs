use auto_ops::impl_op_ex;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Histogram {
    pub counts: Vec<f64>,
    pub edges: Vec<f64>,
    pub errors: Vec<f64>,
}
impl Histogram {
    pub fn new(counts: &[f64], edges: &[f64], errors: Option<&[f64]>) -> Self {
        assert_eq!(counts.len(), edges.len() - 1);
        let errors = errors
            .map(|e| e.to_vec())
            .unwrap_or(counts.iter().map(|c| c.abs().sqrt()).collect::<Vec<f64>>());
        assert_eq!(counts.len(), errors.len());
        Self {
            counts: counts.to_vec(),
            edges: edges.to_vec(),
            errors,
        }
    }
    pub fn empty(edges: &[f64]) -> Self {
        let nbins = edges.len() - 1;
        let low = edges[0];
        let high = edges[nbins];
        let width = (high - low) / nbins as f64;
        let edges = (0..=nbins)
            .map(|i| low + i as f64 * width)
            .collect::<Vec<_>>();
        Self {
            counts: vec![0.0; nbins],
            edges,
            errors: vec![0.0; nbins],
        }
    }
    pub fn bins(&self) -> usize {
        self.edges.len() - 1
    }
    pub fn widths(&self) -> Vec<f64> {
        self.edges.windows(2).map(|w| w[1] - w[0]).collect()
    }
    pub fn centers(&self) -> Vec<f64> {
        self.edges.windows(2).map(|w| 0.5 * (w[0] + w[1])).collect()
    }
    pub fn edges(&self) -> &[f64] {
        &self.edges
    }
    pub fn counts(&self) -> &[f64] {
        &self.counts
    }
    pub fn errors(&self) -> &[f64] {
        &self.errors
    }
    pub fn get_index(&self, value: f64) -> Option<usize> {
        let first = *self.edges.first()?;
        let last = *self.edges.last()?;
        if value < first || value >= last {
            return None;
        }
        match self.edges.binary_search_by(|e| e.total_cmp(&value)) {
            Ok(i) => Some(i.saturating_sub(1).min(self.bins() - 1)),
            Err(i) => Some(i - 1),
        }
    }
}
impl_op_ex!(+ |a: &Histogram, b: &Histogram| -> Histogram {
        assert_eq!(a.edges, b.edges);
        let counts =a
            .counts
            .iter()
            .zip(&b.counts)
            .map(|(a, b)| a + b)
            .collect();
        let errors = a
            .errors
            .iter()
            .zip(&b.errors)
            .map(|(a, b)| a.hypot(*b))
            .collect();
        Histogram {
            counts,
            edges: a.edges.clone(),
            errors,
        }
});
