pub struct Record<'a> {
    pub value: &'a mut [u8],
    pub offset: u64,
}

pub struct Log<'a> {
    pub records: Vec<Record<'a>>,
}
