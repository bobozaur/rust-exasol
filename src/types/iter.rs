use sqlx_core::{
    database::Database,
    encode::{Encode, IsNull},
    types::Type,
};

use crate::{arguments::ExaBuffer, Exasol};

/// Adapter allowing any iterator of encodable values to be passed
/// as a parameter set / array to Exasol.
///
/// Note that the iterator must implement [`Clone`] because
/// it's used in multiple places. Therefore, prefer using iterators over
/// references than owning variants
///
/// ```rust
/// use exasol::ExaIter;
///
/// // Don't do this, as the iterator gets cloned internally.
/// let vector = vec![1, 2, 3];
/// let owned_iter = ExaIter::from(vector);
///
/// // Rather, prefer using something cheaper to clone, like:
/// let vector = vec![1, 2, 3];
/// let borrowed_iter = ExaIter::from(vector.as_slice());
/// ```
pub struct ExaIter<I, T>
where
    I: IntoIterator<Item = T> + Clone,
    for<'q> T: Encode<'q, Exasol> + Type<Exasol>,
{
    value: I,
}

impl<I, T> From<I> for ExaIter<I, T>
where
    I: IntoIterator<Item = T> + Clone,
    for<'q> T: Encode<'q, Exasol> + Type<Exasol>,
{
    fn from(value: I) -> Self {
        Self { value }
    }
}

impl<T, I> Type<Exasol> for ExaIter<I, T>
where
    I: IntoIterator<Item = T> + Clone,
    for<'q> T: Encode<'q, Exasol> + Type<Exasol>,
{
    fn type_info() -> <Exasol as Database>::TypeInfo {
        T::type_info()
    }

    fn compatible(ty: &<Exasol as Database>::TypeInfo) -> bool {
        <Self as Type<Exasol>>::type_info().compatible(ty)
    }
}

impl<T, I> Encode<'_, Exasol> for ExaIter<I, T>
where
    I: IntoIterator<Item = T> + Clone,
    for<'q> T: Encode<'q, Exasol> + Type<Exasol>,
{
    fn produces(&self) -> Option<<Exasol as Database>::TypeInfo> {
        let mut output = None;

        for value in self.value.clone().into_iter() {
            match (&output, value.produces()) {
                (None, Some(new)) => output = Some(new),
                (Some(old), Some(new)) if !old.compatible(&new) => output = Some(new),
                _ => (),
            }
        }

        output.or_else(|| Some(T::type_info()))
    }

    fn encode_by_ref(&self, buf: &mut ExaBuffer) -> IsNull {
        buf.append_iter(self.value.clone().into_iter());
        IsNull::No
    }

    fn size_hint(&self) -> usize {
        self.value
            .clone()
            .into_iter()
            .fold(0, |sum, item| sum + item.size_hint())
    }
}

impl<'a, T> Type<Exasol> for &'a [T]
where
    T: Type<Exasol> + 'a,
{
    fn type_info() -> <Exasol as Database>::TypeInfo {
        T::type_info()
    }

    fn compatible(ty: &<Exasol as Database>::TypeInfo) -> bool {
        <Self as Type<Exasol>>::type_info().compatible(ty)
    }
}

impl<T> Encode<'_, Exasol> for &[T]
where
    for<'q> T: Encode<'q, Exasol> + Type<Exasol>,
{
    fn produces(&self) -> Option<<Exasol as Database>::TypeInfo> {
        let mut output = None;

        for value in self.iter() {
            match (&output, value.produces()) {
                (None, Some(new)) => output = Some(new),
                (Some(old), Some(new)) if !old.compatible(&new) => output = Some(new),
                _ => (),
            }
        }

        output.or_else(|| Some(T::type_info()))
    }

    fn encode_by_ref(&self, buf: &mut ExaBuffer) -> IsNull {
        buf.append_iter(self.iter());
        IsNull::No
    }

    fn size_hint(&self) -> usize {
        self.iter().fold(0, |sum, item| sum + item.size_hint())
    }
}

impl<'a, T> Type<Exasol> for &'a mut [T]
where
    T: Type<Exasol> + 'a,
{
    fn type_info() -> <Exasol as Database>::TypeInfo {
        T::type_info()
    }

    fn compatible(ty: &<Exasol as Database>::TypeInfo) -> bool {
        <Self as Type<Exasol>>::type_info().compatible(ty)
    }
}

impl<T> Encode<'_, Exasol> for &mut [T]
where
    for<'q> T: Encode<'q, Exasol> + Type<Exasol>,
{
    fn produces(&self) -> Option<<Exasol as Database>::TypeInfo> {
        let mut output = None;

        for value in self.iter() {
            match (&output, value.produces()) {
                (None, Some(new)) => output = Some(new),
                (Some(old), Some(new)) if !old.compatible(&new) => output = Some(new),
                _ => (),
            }
        }

        output.or_else(|| Some(T::type_info()))
    }

    fn encode_by_ref(&self, buf: &mut ExaBuffer) -> IsNull {
        buf.append_iter(self.iter());
        IsNull::No
    }

    fn size_hint(&self) -> usize {
        self.iter().fold(0, |sum, item| sum + item.size_hint())
    }
}

impl<T, const N: usize> Type<Exasol> for [T; N]
where
    T: Type<Exasol>,
{
    fn type_info() -> <Exasol as Database>::TypeInfo {
        T::type_info()
    }

    fn compatible(ty: &<Exasol as Database>::TypeInfo) -> bool {
        <Self as Type<Exasol>>::type_info().compatible(ty)
    }
}

impl<T, const N: usize> Encode<'_, Exasol> for [T; N]
where
    for<'q> T: Encode<'q, Exasol> + Type<Exasol>,
{
    fn produces(&self) -> Option<<Exasol as Database>::TypeInfo> {
        let mut output = None;

        for value in self.iter() {
            match (&output, value.produces()) {
                (None, Some(new)) => output = Some(new),
                (Some(old), Some(new)) if !old.compatible(&new) => output = Some(new),
                _ => (),
            }
        }

        output.or_else(|| Some(T::type_info()))
    }

    fn encode_by_ref(&self, buf: &mut ExaBuffer) -> IsNull {
        buf.append_iter(self.iter());
        IsNull::No
    }

    fn size_hint(&self) -> usize {
        self.iter().fold(0, |sum, item| sum + item.size_hint())
    }
}

impl<T> Type<Exasol> for Vec<T>
where
    T: Type<Exasol>,
{
    fn type_info() -> <Exasol as Database>::TypeInfo {
        T::type_info()
    }

    fn compatible(ty: &<Exasol as Database>::TypeInfo) -> bool {
        <Self as Type<Exasol>>::type_info().compatible(ty)
    }
}

impl<T> Encode<'_, Exasol> for Vec<T>
where
    for<'q> T: Encode<'q, Exasol> + Type<Exasol>,
{
    fn produces(&self) -> Option<<Exasol as Database>::TypeInfo> {
        let mut output = None;

        for value in self.iter() {
            match (&output, value.produces()) {
                (None, Some(new)) => output = Some(new),
                (Some(old), Some(new)) if !old.compatible(&new) => output = Some(new),
                _ => (),
            }
        }

        output.or_else(|| Some(T::type_info()))
    }

    fn encode_by_ref(&self, buf: &mut ExaBuffer) -> IsNull {
        buf.append_iter(self.iter());
        IsNull::No
    }

    fn size_hint(&self) -> usize {
        self.iter().fold(0, |sum, item| sum + item.size_hint())
    }
}

impl<T> Type<Exasol> for Box<[T]>
where
    T: Type<Exasol>,
{
    fn type_info() -> <Exasol as Database>::TypeInfo {
        T::type_info()
    }

    fn compatible(ty: &<Exasol as Database>::TypeInfo) -> bool {
        <Self as Type<Exasol>>::type_info().compatible(ty)
    }
}

impl<T> Encode<'_, Exasol> for Box<[T]>
where
    for<'q> T: Encode<'q, Exasol> + Type<Exasol>,
{
    fn produces(&self) -> Option<<Exasol as Database>::TypeInfo> {
        let mut output = None;

        for value in self.iter() {
            match (&output, value.produces()) {
                (None, Some(new)) => output = Some(new),
                (Some(old), Some(new)) if !old.compatible(&new) => output = Some(new),
                _ => (),
            }
        }

        output.or_else(|| Some(T::type_info()))
    }

    fn encode_by_ref(&self, buf: &mut ExaBuffer) -> IsNull {
        buf.append_iter(self.iter());
        IsNull::No
    }

    fn size_hint(&self) -> usize {
        self.iter().fold(0, |sum, item| sum + item.size_hint())
    }
}
