pub fn median(values: &[i32]) -> Option<i32> {
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    sorted.first().copied()
}
