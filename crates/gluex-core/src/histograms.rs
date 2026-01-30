use auto_ops::impl_op_ex;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Histogram {
    pub counts: Vec<f64>,
    pub edges: Vec<f64>,
    pub errors: Vec<f64>,
}
impl Histogram {
    pub fn limits(&self) -> (f64, f64) {
        (self.edges[0], self.edges[self.edges.len() - 1])
    }
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
    pub fn new_filled(data: &[f64], edges: &[f64]) -> Self {
        let mut hist = Self::empty(edges);
        hist.fill_all(data);
        hist
    }
    pub fn new_filled_weighted(data: &[f64], weights: &[f64], edges: &[f64]) -> Self {
        let mut hist = Self::empty(edges);
        hist.fill_all_weighted(data, weights);
        hist
    }
    pub fn new_uniform_filled(data: &[f64], bins: usize, limits: (f64, f64)) -> Self {
        let mut hist = Self::empty_uniform(bins, limits);
        hist.fill_all(data);
        hist
    }
    pub fn new_uniform_filled_weighted(
        data: &[f64],
        weights: &[f64],
        bins: usize,
        limits: (f64, f64),
    ) -> Self {
        let mut hist = Self::empty_uniform(bins, limits);
        hist.fill_all_weighted(data, weights);
        hist
    }
    pub fn new_uniform(counts: &[f64], limits: (f64, f64), errors: Option<&[f64]>) -> Self {
        let bins = counts.len();
        let (min, max) = limits;
        let width = (max - min) / bins as f64;
        let edges: Vec<f64> = (0..=bins).map(|i| min + i as f64 * width).collect();
        Self::new(counts, &edges, errors)
    }
    pub fn empty(edges: &[f64]) -> Self {
        let nbins = edges.len() - 1;
        Self {
            counts: vec![0.0; nbins],
            edges: edges.to_vec(),
            errors: vec![0.0; nbins],
        }
    }
    pub fn empty_uniform(bins: usize, limits: (f64, f64)) -> Self {
        let (min, max) = limits;
        let width = (max - min) / bins as f64;
        let edges: Vec<f64> = (0..=bins).map(|i| min + i as f64 * width).collect();
        Self::empty(&edges)
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
    pub fn fill(&mut self, value: f64) {
        if let Some(ibin) = self.get_index(value) {
            self.counts[ibin] += 1.0;
            self.errors[ibin] = self.errors[ibin].hypot(1.0);
        }
    }
    pub fn fill_all(&mut self, values: &[f64]) {
        for value in values {
            self.fill(*value);
        }
    }
    pub fn fill_weighted(&mut self, value: f64, weight: f64) {
        if let Some(ibin) = self.get_index(value) {
            self.counts[ibin] += weight;
            self.errors[ibin] = self.errors[ibin].hypot(weight);
        }
    }
    pub fn fill_all_weighted(&mut self, values: &[f64], weights: &[f64]) {
        assert_eq!(values.len(), weights.len());
        for (value, weight) in values.iter().zip(weights) {
            self.fill_weighted(*value, *weight);
        }
    }
    pub fn integral(&self) -> f64 {
        self.counts.iter().sum()
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
