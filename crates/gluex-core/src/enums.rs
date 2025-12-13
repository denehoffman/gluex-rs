use strum::VariantArray;

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash, VariantArray)]
pub enum Polarization {
    AMO,
    Para0,
    Perp45,
    Para90,
    Perp135,
}
