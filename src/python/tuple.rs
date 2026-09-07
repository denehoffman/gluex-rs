//! Immutable tuples with Rust-owned element typing for generated stubs.
use pyo3::{
    inspect::{PyStaticConstant, PyStaticExpr},
    prelude::*,
    types::PyTuple,
};

pub struct TypedTuple<T>(pub Vec<T>);
impl<'py, T: IntoPyObject<'py>> IntoPyObject<'py> for TypedTuple<T> {
    type Target = PyTuple;
    type Output = Bound<'py, PyTuple>;
    type Error = PyErr;
    const OUTPUT_TYPE: PyStaticExpr = PyStaticExpr::Subscript {
        value: &PyStaticExpr::Attribute {
            value: &PyStaticExpr::Name { id: "builtins" },
            attr: "tuple",
        },
        slice: &PyStaticExpr::Tuple {
            elts: &[
                T::OUTPUT_TYPE,
                PyStaticExpr::Constant {
                    value: PyStaticConstant::Ellipsis,
                },
            ],
        },
    };
    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        PyTuple::new(py, self.0)
    }
}

pub struct TypedIterator<T>(pub Vec<T>);
impl<'py, T: IntoPyObject<'py>> IntoPyObject<'py> for TypedIterator<T> {
    type Target = pyo3::types::PyIterator;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;
    const OUTPUT_TYPE: PyStaticExpr = PyStaticExpr::Subscript {
        value: &PyStaticExpr::Attribute {
            value: &PyStaticExpr::Name {
                id: "collections.abc",
            },
            attr: "Iterator",
        },
        slice: &T::OUTPUT_TYPE,
    };
    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        PyTuple::new(py, self.0)?.as_any().try_iter()
    }
}
