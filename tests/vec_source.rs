pub struct VecSource<T> {
    items: Vec<T>,
    strict_downstream: bool,
}

impl<T> VecSource<T> {
    fn new(items: Vec<T>) -> Self {
        Self {
            items,
            strict_downstream: false,
        }
    }

    fn strict_downstream(mut self, strict: bool) -> Self {
        self.strict_downstream = strict;
        self
    }
}
