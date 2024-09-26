pub trait Swap {
    type Result;

    fn swap(self) -> Self::Result;
}

impl<T, E> Swap for Option<Result<T, E>> {
    type Result = Result<Option<T>, E>;

    fn swap(self) -> Self::Result {
        match self {
            Some(Ok(value)) => Ok(Some(value)),
            Some(Err(value)) => Err(value),
            None => Ok(None)
        }
    }
}

impl<T, E> Swap for Vec<Result<T, E>> {
    type Result = Result<Vec<T>, Vec<E>>;

    fn swap(self) -> Self::Result {
        let mut values = Vec::<T>::new();
        let mut errors = Vec::<E>::new();

        for result in self {
            match result {
                Ok(value) => values.push(value),
                Err(error) => errors.push(error)
            }
        }

        if errors.is_empty() {
            Ok(values)
        } else {
            Err(errors)
        }
    }
}
